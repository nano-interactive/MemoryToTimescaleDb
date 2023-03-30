package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
)

func TestClose(t *testing.T) {
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

	m.Inc("one")
	m.Inc("two")
	m.Inc("three")
	m.Inc("four")
	m.Inc("three")
	m.Inc("four")

	checkOne, _ := m.container.Load().Load("one")
	checkFour, _ := m.container.Load().Load("four")
	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")
	assert.Equal(uint64(1), checkOne.(*atomic.Uint64).Load())
	assert.Equal(uint64(2), checkFour.(*atomic.Uint64).Load())
	assert.Equal(uint64(4), m.containerLen.Load())

	_ = m.Close()

	assert.Equal(uint64(4), insertInc.Load())
	assert.Equal(uint64(0), m.containerLen.Load())

	m.Inc("one")
	_, ok := m.container.Load().Load("one")
	assert.False(ok)

}
