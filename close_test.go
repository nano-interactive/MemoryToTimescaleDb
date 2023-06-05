package mtsdb

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestClose(t *testing.T) {
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		InsertDuration:  10 * time.Minute,
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		skipValidation:  true,
	}
	ctx := context.Background()
	m, err := newMtsdb(ctx, nil, tstConfig)
	assert.NoError(err)

	c, err := NewMetricCounter(ctx, "testCounter", MetricCounterConfig{}, "label1")
	assert.NoError(err)

	m.MustRegister(c)

	go func() {
		for job := range m.job {
			insertInc.Add(uint64(job.Len()))
			m.wg.Done()
		}
	}()

	c.Inc("one")
	c.Inc("two")
	c.Inc("three")
	c.Inc("four")
	c.Inc("three")
	c.Inc("four")

	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")

	_ = m.Close()

	assert.Equal(uint64(4), insertInc.Load())

	c.Inc("one")

	value, ok := c.Get("one")
	assert.True(ok)
	assert.Equal(uint32(1), value)

	_, ok = c.Get("two")
	assert.False(ok)

}
