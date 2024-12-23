package proto

import chproto "github.com/ClickHouse/ch-go/proto"

// 必须保证 Final Block 中 row number > 0
type FinalBlock struct {
	buffer       *chproto.Buffer
	startIndices []int
	query        string
}

func (b *FinalBlock) GetQuery() string {
	return b.query
}
