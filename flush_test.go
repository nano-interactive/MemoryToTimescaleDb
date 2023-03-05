package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestFlush(t *testing.T) {
	var mu = sync.Mutex{}
	assert := require.New(t)

	insertInc := 0

	tstConfig := Config{
		Size:      5,
		InsertSQL: "",
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		mu.Lock()
		defer mu.Unlock()
		insertInc += batch.Len()
	}

	m.Inc("one")
	m.Inc("two")
	m.Inc("three")
	m.Inc("four")
	m.Inc("three")
	m.Inc("four")

	assert.Equal(0, insertInc, "bulk insert should not be called")
	assert.Equal(uint64(1), m.container["one"].Load())
	assert.Equal(uint64(2), m.container["four"].Load())

	m.Flush()
	time.Sleep(2 * time.Millisecond)
	mu.Lock()
	assert.Equal(4, insertInc, "bulk insert should be called once")
	assert.Equal(0, len(m.container))
	mu.Unlock()
	assert.Nil(m.container["one"])

	assert.Empty(m.container)

}
