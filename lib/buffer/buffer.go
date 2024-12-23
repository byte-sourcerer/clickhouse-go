package proto

import (
	"sync"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/pkg/errors"
)

var pool sync.Pool



// 必须保证 Final Block 中 row number > 0
// todo: reuse buffer
type Buffer struct {
	buffer       *chproto.Buffer
	startIndices []int
	query        string
	compressor   *compress.Writer
}

func (b *Buffer) reset() {
	// reset buffer
	{
		if b.buffer == nil {
			b.buffer = new(chproto.Buffer)
		}
		b.buffer.Reset()
	}

	// reset startIndices
	{
		if b.startIndices == nil {
			b.startIndices = make([]int, 0)
		}

		b.startIndices = b.startIndices[:0]
	}

	b.query = ""

	// reset compressor
	{
		if b.compressor == nil {
			b.compressor = compress.NewWriter()
		}
	}
}

func (b *Buffer) TryInit(
	block proto.Block,
	name string,
	revision uint64,
	maxCompressionBuffer int,
	compression compress.Method,
	query string,
) error {
	b.query = query

	b.buffer.PutByte(proto.ClientData)
	b.buffer.PutString(name)

	compressionOffset := len(b.buffer.Buf)

	if err := block.EncodeHeader(b.buffer, revision); err != nil {
		return err
	}

	split := true

	for i := range block.Columns {
		if split {
			b.startIndices = append(b.startIndices, 0)
			split = false
		}

		if err := block.EncodeColumn(b.buffer, revision, i); err != nil {
			return err
		}
		if len(b.buffer.Buf) >= maxCompressionBuffer {
			if err := b.compressBuffer(compressionOffset, compression); err != nil {
				return err
			}

			b.startIndices = append(b.startIndices, len(b.buffer.Buf))

			split = true
			compressionOffset = len(b.buffer.Buf)
		}
	}

	return nil
}

func (b *Buffer) compressBuffer(start int, compression compress.Method) error {
	if compression != compress.None {
		compressed, err := doCompress(b.compressor, compression, b.buffer.Buf[start:])
		if err != nil {
			return err
		}
		b.buffer.Buf = append(b.buffer.Buf[:start], compressed...)
	}
	return nil
}

func doCompress(compressor *compress.Writer, compression compress.Method, data []byte) ([]byte, error) {
	if err := compressor.Compress(compression, data); err != nil {
		return nil, errors.Wrap(err, "compress")
	}

	return compressor.Data, nil
}

func (b *Buffer) GetQuery() string {
	return b.query
}

func (b *Buffer) GetNumBlocks() int {
	return len(b.startIndices)
}

func (b *Buffer) GetBlock(i int) []byte {
	begin := b.getBegin(i)
	end := b.getEnd(i)
	return b.buffer.Buf[begin:end]
}

func (b *Buffer) getBegin(i int) int {
	return b.startIndices[i]
}

func (b *Buffer) getEnd(i int) int {
	if i < len(b.startIndices)-1 {
		return b.startIndices[i+1]
	} else {
		return len(b.buffer.Buf)
	}
}
