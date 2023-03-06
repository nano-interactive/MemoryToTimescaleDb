package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"testing"
)

var testCntBulkLen int

func TestInsert(t *testing.T) {
	assert := require.New(t)

	tstConfig := Config{
		Size:      3,
		InsertSQL: "",
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		testCntBulkLen += batch.Len()
	}

	param := map[string]int{
		"one":   1,
		"two":   2,
		"three": 3,
		"four":  4,
	}
	m.insert(param)

	assert.Equal(4, testCntBulkLen)

}
