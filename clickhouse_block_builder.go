package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type blockBuilder struct {
	query     string
	block     *proto.Block
	structMap *structMap
	debugf    func(format string, v ...any)
}

func (b *blockBuilder) Append(v ...any) error {
	// 不支持流式协议，理论上应该开个新接口……
	// if len(v) > 0 {
	// 	if r, ok := v[0].(*rows); ok {
	// 		return b.appendRowsBlocks(r)
	// 	}
	// }

	return b.block.Append(v...)
}

func (b *blockBuilder) AppendStruct(v any) error {
	values, err := b.structMap.Map("AppendStruct", b.block.ColumnsNames(), v, false)
	if err != nil {
		return err
	}
	return b.Append(values...)
}

func (b *blockBuilder) Column(idx int) driver.BatchColumn {
	if len(b.block.Columns) <= idx {
		err := &OpError{
			Op:  "batch.Column",
			Err: fmt.Errorf("invalid column index %d", idx),
		}

		return &batchColumn{
			err: err,
		}
	}
	return &batchColumn{
		column: b.block.Columns[idx],
	}
}
