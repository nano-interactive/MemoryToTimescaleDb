package mtsdb

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStats(t *testing.T) {
	assert := require.New(t)

	tstConfig := Config{
		Size:            5,
		InsertSQL:       "test",
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		skipValidation:  true,
	}
	m, err := newMtsdb(context.Background(), nil, tstConfig)
	assert.NoError(err)
	go func() {
		for job := range m.job {
			m.MetricInserts.Add(uint64(job.Len()))
			m.MetricDurationMs.Add(uint64(100_000))
			m.wg.Done()
		}
	}()

	m.Inc("one")
	m.Inc("two")
	m.Inc("three")
	m.Inc("four")
	m.Inc("three")
	m.Inc("four")
	m.Inc("five")

	m.wg.Wait()
	inserts, dur := m.Stats()

	assert.Equal(uint64(5), inserts)
	assert.Equal(uint64(100_000), dur)

	_ = m.Close()
}

func TestStatsReset(t *testing.T) {
	assert := require.New(t)

	tstConfig := Config{
		skipValidation: true,
	}
	m, err := newMtsdb(context.Background(), nil, tstConfig)
	assert.NoError(err)

	m.MetricDurationMs.Store(1e15 + 1)
	m.MetricInserts.Store(218)
	ins, dur := m.Stats()
	ins, dur = m.Stats()
	assert.Equal(uint64(0), ins)
	assert.Equal(uint64(0), dur)

	err = m.Close()
	assert.NoError(err)
}
