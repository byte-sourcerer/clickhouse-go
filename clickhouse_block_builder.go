package clickhouse

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/pkg/errors"
)

type blockBuilder struct {
	query  string
	block  *proto.Block
	debugf func(format string, v ...any)
}

func (b *blockBuilder) Append(v ...any) error {
	if len(v) > 0 {
		if r, ok := v[0].(*rows); ok {
			return b.appendRowsBlocks(r)
		}
	}

	if err := b.block.Append(v...); err != nil {
		b.err = errors.Wrap(ErrBatchInvalid, err.Error())
		b.release(err)
		return err
	}
	return nil
}

// appendRowsBlocks is an experimental feature that allows rows blocks be appended directly to the batch.
// This API is not stable and may be changed in the future.
// See: tests/batch_block_test.go
func (b *blockBuilder) appendRowsBlocks(r *rows) error {
	var lastReadLock *proto.Block
	var blockNum int

	for r.Next() {
		if lastReadLock == nil { // make sure the first block is logged
			b.debugf("[batch.appendRowsBlocks] blockNum = %d", blockNum)
		}

		// rows.Next() will read the next block from the server only if the current block is empty
		// only if new block is available we should flush the current block
		// the last block will be handled by the batch.Send() method
		if lastReadLock != nil && lastReadLock != r.block {
			if err := b.Flush(); err != nil {
				return err
			}
			blockNum++
			b.debugf("[batch.appendRowsBlocks] blockNum = %d", blockNum)
		}

		b.block = r.block
		lastReadLock = r.block
	}

	return nil
}
