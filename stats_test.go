package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	assert := require.New(t)

	tstConfig := Config{
		Size:      5,
		InsertSQL: "test",
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		m.MetricInserts.Add(uint64(batch.Len()))
		m.MetricDurationMs.Add(uint64(100_000))
	}

	m.Inc("one")
	m.Inc("two")
	m.Inc("three")
	m.Inc("four")
	m.Inc("three")
	m.Inc("four")
	m.Inc("five")

	time.Sleep(2 * time.Millisecond)
	inserts, dur := m.Stats()

	assert.Equal(uint64(5), inserts)
	assert.Equal(uint64(100_000), dur)

	_ = m.Close()
}
