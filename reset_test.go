package mtsdb

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestReset(t *testing.T) {
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		TableName:       "test",
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		InsertDuration:  10 * time.Minute,
		skipValidation:  true,
	}
	m, err := newMtsdb(context.Background(), nil, tstConfig, "url")
	assert.NoError(err)

	go func() {
		for job := range m.job {
			insertInc.Add(uint64(job.Len()))
			m.wg.Done()
		}
	}()

	m.Inc("one")
	m.Inc("one")

	m.reset()

	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")
	_, err = m.fetchMetricValue("one")
	assert.ErrorIs(err, MetricNotFound)

	_ = m.Close()
}
