package tests

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	bf "github.com/ClickHouse/clickhouse-go/v2/lib/buffer"

	"github.com/stretchr/testify/require"
)

func TestSplitSend(t *testing.T) {
	conn, err := GetNativeConnectionWithMaxCompressionBuffer(
		clickhouse.Settings{
			"max_execution_time": 60,
		},
		nil,
		&clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		100,
	)
	require.NoError(t, err)

	conn.Exec(context.Background(), "DROP TABLE IF EXISTS example")
	require.NoError(t, conn.Exec(
		context.Background(),
		`
			CREATE TABLE example (
				i64 Int64,
				s String,
				low_s LowCardinality(String),
				msg String CODEC(ZSTD(1)),
				msgOver String CODEC(ZSTD(5))
			)
			ENGINE = Memory
			`,
	))

	buildRandomRow := func() []any {
		return []any{
			int64(rand.Int()),
			randomStr(),
			randomStr(),
			randomStr(),
			randomStr(),
		}
	}

	sendOnce := func() {
		batchBuilder, sender, err := conn.PrepareBatchBuilderAndSender(context.Background(), "INSERT INTO example")
		require.NoError(t, err)

		for i := 0; i < 10000; i++ {
			err := batchBuilder.Append(buildRandomRow()...)
			require.NoError(t, err)
		}

		buffer := bf.GetBuffer()
		defer bf.PutBuffer(buffer)

		buffer, err = batchBuilder.Build(buffer)
		require.NoError(t, err)

		err = sender.Send(context.Background(), buffer)
		require.NoError(t, err)
	}

	wg := &sync.WaitGroup{}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 100; i++ {
				sendOnce()
			}
		}()
	}

	wg.Wait()
}

func randomStr() string {
	s := []string{"a", "arfghqarhfp;a", "ahfidia;dhfaip", "ghilustghhuhr", "fdhgias", "arhfdpahjrfphsa"}
	i := rand.Int() % len(s)
	return s[i]
}
