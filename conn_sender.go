package clickhouse

import (
	"context"
	"os"
)

type sender struct {
	err          error
	ctx          context.Context
	conn         *connect
	sent         bool // sent signalize that batch is send to ClickHouse.
	released     bool // released signalize that conn was returned to pool and can't be used.
	closeOnFlush bool // closeOnFlush signalize that batch should close query and release conn when use Flush
	connRelease  func(*connect, error)
	connAcquire  func(context.Context) (*connect, error)
	onProcess    *onProcess
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
