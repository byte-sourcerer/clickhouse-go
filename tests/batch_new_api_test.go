package tests

import (
	"context"
	"math/big"
	"math/rand"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	bf "github.com/ClickHouse/clickhouse-go/v2/lib/buffer"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiBufferSend(t *testing.T) {
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

	buildRow := func() []any {
		return []any{
			3,
			"hello world",
			"hello world",
			"hello world",
			"hello world",
		}
	}

	{
		batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO example")
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			err := batch.Append(buildRow()...)
			require.NoError(t, err)
		}

		require.NoError(t, batch.Send())
	}

	{
		batchBuilder, sender, err := conn.PrepareBatchBuilderAndSender(context.Background(), "INSERT INTO example")
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			err := batchBuilder.Append(buildRow()...)
			require.NoError(t, err)
		}

		buffer := bf.GetBuffer()
		defer bf.PutBuffer(buffer)

		buffer, err = batchBuilder.Build(buffer)
		require.NoError(t, err)

		err = sender.Send(context.Background(), buffer)
		require.NoError(t, err)
	}
}

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

	for i := 0; i < 10; i++ {
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

const insertSql = "INSERT INTO example"

func TestNewAPIBigInt(t *testing.T) {
	require.NoError(t, ReadWriteBigIntNewAPI(t))
}

func TestNewAPIPrepareTwice(t *testing.T) {
	require.NoError(t, prepareTwice(t))
}

func TestNewAPISendTwice(t *testing.T) {
	require.NoError(t, sendTwice(t))
}

func TestNewAPIManyRows(t *testing.T) {
	require.NoError(t, sendMany(t, 100000, false))
	require.NoError(t, sendMany(t, 100000, true))
}

func ReadWriteBigIntNewAPI(t *testing.T) error {
	ctx := context.Background()

	conn, err := createTableForNewApiTest(ctx)
	if err != nil {
		return err
	}

	builder, sender, err := conn.PrepareBatchBuilderAndSender(ctx, insertSql)
	if err != nil {
		return err
	}

	if err := builder.Append(buildRow()...); err != nil {
		return err
	}

	buffer := bf.GetBuffer()
	defer bf.PutBuffer(buffer)

	buffer, err = builder.Build(buffer)
	if err != nil {
		return err
	}

	if err := sender.Send(context.Background(), buffer); err != nil {
		return err
	}

	rows, err := conn.Query(ctx, "SELECT * FROM example")
	if err != nil {
		return err
	}

	assertRows(t, rows, 1)

	return nil
}

func sendTwice(t *testing.T) error {
	ctx := context.Background()

	conn, err := createTableForNewApiTest(ctx)
	if err != nil {
		return err
	}

	builder, sender, err := conn.PrepareBatchBuilderAndSender(ctx, insertSql)
	if err != nil {
		return err
	}

	if err := builder.Append(buildRow()...); err != nil {
		return err
	}

	buffer := bf.GetBuffer()
	defer bf.PutBuffer(buffer)

	buffer, err = builder.Build(buffer)
	if err != nil {
		return err
	}

	// send first
	if err := sender.Send(context.Background(), buffer); err != nil {
		return err
	}

	// send twice
	if err := sender.Send(context.Background(), buffer); err != nil {
		return err
	}

	rows, err := conn.Query(ctx, "SELECT * FROM example")
	if err != nil {
		return err
	}

	assertRows(t, rows, 2)

	return nil
}

func prepareTwice(t *testing.T) error {
	ctx := context.Background()

	conn, err := createTableForNewApiTest(ctx)
	if err != nil {
		return err
	}

	prepareAndSend := func() error {
		t.Logf("PrepareBatchBuilderAndSender")
		builder, sender, err := conn.PrepareBatchBuilderAndSender(ctx, insertSql)
		if err != nil {
			return err
		}

		t.Logf("Append")
		if err := builder.Append(buildRow()...); err != nil {
			return err
		}

		buffer := bf.GetBuffer()
		defer bf.PutBuffer(buffer)

		if buffer == nil {
			panic("buffer is nil")
		}

		t.Logf("Build buffer")
		buffer, err = builder.Build(buffer)
		if err != nil {
			return err
		}

		t.Logf("Send")
		if err := sender.Send(context.Background(), buffer); err != nil {
			return err
		}

		return nil
	}

	if err := prepareAndSend(); err != nil {
		return err
	}

	if err := prepareAndSend(); err != nil {
		return err
	}

	rows, err := conn.Query(ctx, "SELECT * FROM example")
	if err != nil {
		return err
	}

	assertRows(t, rows, 2)

	return nil
}

func sendMany(t *testing.T, numRows int, resend bool) error {
	ctx := context.Background()

	conn, err := createTableForNewApiTest(ctx)
	if err != nil {
		return err
	}

	builder, sender, err := conn.PrepareBatchBuilderAndSender(ctx, insertSql)
	if err != nil {
		return err
	}

	for i := 0; i < numRows; i++ {
		if err := builder.Append(buildRow()...); err != nil {
			return err
		}
	}

	buffer := bf.GetBuffer()
	defer bf.PutBuffer(buffer)

	buffer, err = builder.Build(buffer)
	if err != nil {
		return err
	}

	if err := sender.Send(context.Background(), buffer); err != nil {
		return err
	}

	if resend {
		if err := sender.Send(context.Background(), buffer); err != nil {
			return err
		}
	}

	rows, err := conn.Query(ctx, "SELECT * FROM example")
	if err != nil {
		return err
	}

	expectedNumRows := numRows
	if resend {
		expectedNumRows = 2 * numRows
	}

	assertRows(t, rows, expectedNumRows)

	return nil
}

func createTableForNewApiTest(ctx context.Context) (driver.Conn, error) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	if err != nil {
		return nil, err
	}
	conn.Exec(ctx, "DROP TABLE IF EXISTS example")

	if err = conn.Exec(ctx, `
		CREATE TABLE example (
			Col1 Int128,
			Col2 UInt128,
			Col3 Array(Int128),
			Col4 Int256,
			Col5 Array(Int256),
			Col6 UInt256,
			Col7 Array(UInt256)
		) Engine Memory`); err != nil {
		return nil, err
	}

	return conn, nil
}

func buildRow() []any {
	col1Data, _ := new(big.Int).SetString("170141183460469231731687303715884105727", 10)
	col2Data := big.NewInt(128)
	col3Data := []*big.Int{
		big.NewInt(-128),
		big.NewInt(128128),
		big.NewInt(128128128),
	}
	col4Data := big.NewInt(256)
	col5Data := []*big.Int{
		big.NewInt(256),
		big.NewInt(256256),
		big.NewInt(256256256256),
	}
	col6Data := big.NewInt(256)
	col7Data := []*big.Int{
		big.NewInt(256),
		big.NewInt(256256),
		big.NewInt(256256256256),
	}

	return []any{col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data}
}

func assertRows(t *testing.T, rows driver.Rows, expectedCount int) {
	expectedRow := buildRow()

	count := 0
	for rows.Next() {
		count += 1

		var (
			col1 big.Int
			col2 big.Int
			col3 []*big.Int
			col4 big.Int
			col5 []*big.Int
			col6 big.Int
			col7 []*big.Int
		)

		err := rows.Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7)
		require.NoError(t, err)

		// t.Logf("col1=%v, col2=%v, col3=%v, col4=%v, col5=%v, col6=%v, col7=%v\n", col1.String(), col2, col3, col4, col5, col6, col7)

		{
			assert.Equal(t, expectedRow[0], &col1)
			assert.Equal(t, expectedRow[1], &col2)
			assert.Equal(t, expectedRow[2], col3)
			assert.Equal(t, expectedRow[3], &col4)
			assert.Equal(t, expectedRow[4], col5)
			assert.Equal(t, expectedRow[5], &col6)
			assert.Equal(t, expectedRow[6], col7)
		}
	}

	assert.Equal(t, expectedCount, count)
}
