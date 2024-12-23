package clickhouse

import (
	"github.com/ClickHouse/ch-go/compress"
	bf "github.com/ClickHouse/clickhouse-go/v2/lib/buffer"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/pkg/errors"
)

type BatchBuilder struct {
	block                *proto.Block
	query                string
	revision             uint64
	maxCompressionBuffer int
	compression          compress.Method
	debugf               func(format string, v ...any)
}

var _ driver.BatchBuilder = (*BatchBuilder)(nil)

func (b *BatchBuilder) Append(v ...any) error {
	if err := b.block.Append(v...); err != nil {
		return errors.Wrap(ErrBatchInvalid, err.Error())
	} else {
		return nil
	}
}

func (b *BatchBuilder) Build(destination *bf.Buffer) (*bf.Buffer, error) {
	err := destination.TryInit(*b.block, "", b.revision, b.maxCompressionBuffer, b.compression, b.query)
	return destination, err
}
