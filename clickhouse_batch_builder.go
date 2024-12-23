package clickhouse

import (
	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/pkg/errors"
)

// todo: reuse buffer
type BatchBuilder struct {
	block                *proto.Block
	query                string
	revision             uint64
	maxCompressionBuffer int
	compression          compress.Method
	debugf               func(format string, v ...any)
}

func (b *BatchBuilder) Append(v ...any) error {
	err := b.block.Append(v...)
	return errors.Wrap(ErrBatchInvalid, err.Error())
}

func (b *BatchBuilder) Build(destination *proto.Buffer) (*proto.Buffer, error) {
	err := destination.TryInit(*b.block, "", b.revision, b.maxCompressionBuffer, b.compression, b.query)
	return destination, err
}
