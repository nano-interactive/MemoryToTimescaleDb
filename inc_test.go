package mtsdb

import (
	"context"
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
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		skipValidation:  true,
	}
	m, err := newMtsdb(context.Background(), nil, tstConfig)
	assert.NoError(err)

	go func() {
		for job := range m.job {
			insertInc.Add(uint64(job.Len()))
			m.wg.Done()
		}
	}()

	m.Inc("")
	m.Inc("")

	assert.Equal(uint64(0), insertInc.Load())
	assert.Equal(uint64(0), m.containerLen.Load())

	_ = m.Close()
}
