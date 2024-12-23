package clickhouse

import (
	"context"
	"io"
	"os"
	"syscall"

	bf "github.com/ClickHouse/clickhouse-go/v2/lib/buffer"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/pkg/errors"
)

type sender struct {
	sender driver.Sender
	conn   driver.Conn
}

var _ (driver.Sender) = (*sender)(nil)

func (s *sender) Abort() error {
	if s.sender == nil {
		return nil
	}

	return s.sender.Abort()
}

func (s *sender) Send(ctx context.Context, block *bf.Buffer) error {
	if s.sender == nil {
		newSender, err := s.conn.AcquireSender(ctx)
		if err != nil {
			return err
		}
		s.sender = newSender
	}

	err := s.sender.Send(ctx, block)
	s.sender = nil
	return err
}

// onceSender 是一次性的 Sender
type onceSender struct {
	conn        *connect
	connRelease func(*connect, error)

	onProcess *onProcess
	debugf    func(format string, v ...any)
}

var _ (driver.Sender) = (*onceSender)(nil)

// Abort takes the ownership of s, and must not be called twice
func (s *onceSender) Abort() error {
	s.release(os.ErrProcessDone)
	return nil
}

// Send takes the ownership of s, and must not be called twice
func (s *onceSender) Send(ctx context.Context, block *bf.Buffer) (err error) {
	stopCW := contextWatchdog(ctx, func() {
		// close TCP connection on context cancel. There is no other way simple way to interrupt underlying operations.
		// as verified in the test, this is safe to do and cleanups resources later on
		if s.conn != nil {
			_ = s.conn.conn.Close()
		}
	})

	defer func() {
		stopCW()
		s.release(err)
	}()

	if err = s.sendData(block); err != nil {
		return err
	}
	if err = s.closeQuery(ctx); err != nil {
		return err
	}
	return nil
}

func (s *onceSender) sendData(blocks *bf.Buffer) error {
	if blocks.GetNumBlocks() == 0 {
		panic("bug: blocks is empty")
	}

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

func (s *onceSender) closeQuery(ctx context.Context) error {
	if err := s.conn.sendData(&proto.Block{}, ""); err != nil {
		return err
	}

	if err := s.conn.process(ctx, s.onProcess); err != nil {
		return err
	}

	return nil
}

func (s *onceSender) release(err error) {
	s.connRelease(s.conn, err)
	s.connRelease = nil
}
