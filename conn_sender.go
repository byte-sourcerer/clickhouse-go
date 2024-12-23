package clickhouse

import (
	"context"
	"fmt"
	"io"
	"os"
	"syscall"

	bf "github.com/ClickHouse/clickhouse-go/v2/lib/buffer"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/pkg/errors"
)

// OnceSender 只能调用一次
type OnceSender struct {
	conn        *connect
	connRelease func(*connect, error)
	used        bool

	onProcess *onProcess
	debugf    func(format string, v ...any)
}

var _ (driver.OnceSender) = (*OnceSender)(nil)

// Abort takes the ownership of s, and must not be called twice
func (s *OnceSender) Abort() error {
	if s.used {
		return fmt.Errorf("Abort must be called only once")
	}
	s.used = true
	s.release(os.ErrProcessDone)
	return nil
}

// Send takes the ownership of s, and must not be called twice
func (s *OnceSender) Send(ctx context.Context, block *bf.Buffer) (err error) {
	if s.used {
		return fmt.Errorf("Send must be called only once")
	}

	stopCW := contextWatchdog(ctx, func() {
		// close TCP connection on context cancel. There is no other way simple way to interrupt underlying operations.
		// as verified in the test, this is safe to do and cleanups resources later on
		if s.conn != nil {
			_ = s.conn.conn.Close()
		}
	})

	defer func() {
		s.used = true
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

func (s *OnceSender) sendData(blocks *bf.Buffer) error {
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

func (s *OnceSender) closeQuery(ctx context.Context) error {
	if err := s.conn.sendData(&proto.Block{}, ""); err != nil {
		return err
	}

	if err := s.conn.process(ctx, s.onProcess); err != nil {
		return err
	}

	return nil
}

func (s *OnceSender) release(err error) {
	s.connRelease(s.conn, err)
	s.connRelease = nil
}
