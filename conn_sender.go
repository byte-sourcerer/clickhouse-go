package clickhouse

import (
	"context"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/pkg/errors"
)

type sender struct {
	// err          error 这里应该不需要保存 error
	ctx          context.Context
	conn         *connect
	sent         bool // sent signalize that batch is send to ClickHouse.
	released     bool // released signalize that conn was returned to pool and can't be used.
	closeOnFlush bool // closeOnFlush signalize that batch should close query and release conn when use Flush
	connRelease  func(*connect, error)
	connAcquire  func(context.Context) (*connect, error)
	onProcess    *onProcess
	debugf       func(format string, v ...any)
}

func (s *sender) release(err error) {
	if !s.released {
		s.released = true
		s.connRelease(s.conn, err)
	}
}

func (s *sender) Abort() error {
	defer func() {
		s.sent = true
		s.release(os.ErrProcessDone)
	}()
	if s.sent {
		return ErrBatchAlreadySent
	}
	return nil
}

func (s *sender) Send(ctx context.Context, block proto.FinalBlock) (err error) {
	stopCW := contextWatchdog(ctx, func() {
		// close TCP connection on context cancel. There is no other way simple way to interrupt underlying operations.
		// as verified in the test, this is safe to do and cleanups resources later on
		if s.conn != nil {
			_ = s.conn.conn.Close()
		}
	})

	defer func() {
		stopCW()
		s.sent = true
		s.release(err)
	}()
	// if s.err != nil {
	// 	return s.err
	// }
	if s.sent || s.released {
		if err = s.resetConnection(block); err != nil {
			return err
		}
	}

	if err = s.sendData(block); err != nil {
		// there might be an error caused by context cancellation
		// in this case we should return context error instead of net.OpError
		// todo: 似乎没有必要
		// if ctxErr := b.ctx.Err(); ctxErr != nil {
		// 	return ctxErr
		// }

		return err
	}
	if err = s.closeQuery(); err != nil {
		return err
	}
	return nil
}

func (s *sender) sendData(blocks proto.FinalBlock) error {
	// todo: assert block non empty

	// todo: in go1.23, we can use iterators...
	for i := 0; i < blocks.GetNumBlocks(); i++ {
		buffer := blocks.GetBlock(i)
		if err := FlushBuffer(s.conn.conn, buffer); err != nil {
			switch {
			case errors.Is(err, syscall.EPIPE):
				s.debugf("[send data] pipe is broken, closing connection")
				s.conn.setClosed()
			case errors.Is(err, io.EOF):
				s.debugf("[send data] unexpected EOF, closing connection")
				s.conn.setClosed()
			default:
				s.debugf("[send data] unexpected error: %v", err)
			}
			return err
		}
	}

	return nil
}

func (s *sender) resetConnection(block proto.FinalBlock) (err error) {
	// acquire a new conn
	if s.conn, err = s.connAcquire(s.ctx); err != nil {
		return err
	}

	defer func() {
		s.released = false
	}()

	options := queryOptions(s.ctx)
	if deadline, ok := s.ctx.Deadline(); ok {
		s.conn.conn.SetDeadline(deadline)
		defer s.conn.conn.SetDeadline(time.Time{})
	}

	if err = s.conn.sendQuery(block.GetQuery(), &options); err != nil {
		s.release(err)
		return err
	}

	if _, err = s.conn.firstBlock(s.ctx, s.onProcess); err != nil {
		s.release(err)
		return err
	}

	return nil
}

func (s *sender) closeQuery() error {
	if err := s.conn.sendData(&proto.Block{}, ""); err != nil {
		return err
	}

	if err := s.conn.process(s.ctx, s.onProcess); err != nil {
		return err
	}

	return nil
}
