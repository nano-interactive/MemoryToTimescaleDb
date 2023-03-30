package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
)

func TestIncEmptyString(t *testing.T) {
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		Size:            5,
		InsertSQL:       "test",
		WorkerPoolSize:  5,
		BatchInsertSize: 1000,
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		insertInc.Add(uint64(batch.Len()))
	}

	m.Inc("")
	m.Inc("")

	assert.Equal(uint64(0), insertInc.Load())
	assert.Equal(uint64(0), m.containerLen.Load())

	_ = m.Close()
}
