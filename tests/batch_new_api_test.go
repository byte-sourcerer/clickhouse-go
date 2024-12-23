package tests

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"

	bf "github.com/ClickHouse/clickhouse-go/v2/lib/buffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	if err := os.Setenv("CLICKHOUSE_USE_DOCKER", "false"); err != nil {
		panic(err)
	}

	if err := os.Setenv("CLICKHOUSE_PORT", "19000"); err != nil {
		panic(err)
	}
}

func TestBigIntNewAPI(t *testing.T) {
	require.NoError(t, ReadWriteBigIntNewAPI(t))
}

func ReadWriteBigIntNewAPI(t *testing.T) error {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
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
		return err
	}

	builder, sender, err := conn.PrepareBatchBuilderAndSender(ctx, "INSERT INTO example")
	if err != nil {
		return err
	}

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

	if err := builder.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data); err != nil {
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

	var (
		col1 big.Int
		col2 big.Int
		col3 []*big.Int
		col4 big.Int
		col5 []*big.Int
		col6 big.Int
		col7 []*big.Int
	)

	if err = conn.QueryRow(ctx, "SELECT * FROM example").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7); err != nil {
		return err
	}
	assert.Equal(t, *col1Data, col1)
	assert.Equal(t, *col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, *col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, *col6Data, col6)
	assert.Equal(t, col7Data, col7)

	fmt.Printf("col1=%v, col2=%v, col3=%v, col4=%v, col5=%v, col6=%v, col7=%v\n", col1.String(), col2, col3, col4, col5, col6, col7)
	return nil
}
