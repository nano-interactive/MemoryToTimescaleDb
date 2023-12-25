package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	assert := require.New(t)

	m, err := newMtsdb(context.Background(), &pgxpool.Pool{}, DefaultConfig())
	assert.NoError(err)

	m.MetricInserts.Add(uint64(5))
	m.MetricDurationMs.Add(uint64(100_000))

	inserts, dur := m.Stats()

	assert.Equal(uint64(5), inserts)
	assert.Equal(uint64(100_000), dur)

	_ = m.Close()
}

func TestStatsReset(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	tstConfig := Config{
		InsertDuration: 10 * time.Minute,
		skipValidation: true,
	}
	m, err := newMtsdb(context.Background(), nil, tstConfig)
	assert.NoError(err)

	m.MetricDurationMs.Store(1e15 + 1)
	m.MetricInserts.Store(218)
	ins, dur := m.Stats()
	assert.Equal(uint64(0), ins)
	assert.Equal(uint64(0), dur)

	err = m.Close()
	assert.NoError(err)
}
