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

func (b *FinalBlock) GetNumBlocks() int {
	return len(b.startIndices)
}

func (b *FinalBlock) GetBlock(i int) []byte {
	begin := b.getBegin(i)
	end := b.getEnd(i)
	return b.buffer.Buf[begin:end]
}

func (b *FinalBlock) getBegin(i int) int {
	return b.startIndices[i]
}

func (b *FinalBlock) getEnd(i int) int {
	if i < len(b.startIndices)-1 {
		return b.startIndices[i+1]
	} else {
		return len(b.buffer.Buf)
	}
}
