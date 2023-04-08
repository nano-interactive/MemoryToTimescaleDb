package mtsdb

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlush(t *testing.T) {
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		TableName:       "test",
		InsertDuration:  10 * time.Minute,
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
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
	m.Inc("two")
	m.Inc("three")
	m.Inc("four")
	m.Inc("three")
	m.Inc("four")

	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")

	m.Flush()
	m.wg.Wait()

	assert.Equal(uint64(4), insertInc.Load())

	m.Inc("one")
	m.Inc("one")
	checkOne, _ := m.fetchMetricValue("one")
	assert.Equal(uint32(2), checkOne)

	_, err = m.fetchMetricValue("two")
	assert.ErrorIs(err, MetricNotFound)

	_ = m.Close()

}
