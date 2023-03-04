package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"sync/atomic"
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

	param := map[string]*atomic.Uint64{
		"one":   {},
		"two":   {},
		"three": {},
		"four":  {},
	}
	m.insert(param)

	assert.Equal(4, testCntBulkLen)

}
