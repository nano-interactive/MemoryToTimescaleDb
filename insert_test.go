package mtsdb

import (
	"context"
	"github.com/stretchr/testify/require"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestInsert(t *testing.T) {
	testCntBulkLen := atomic.Uint64{}
	assert := require.New(t)

	tstConfig := Config{
		Size:            3,
		InsertSQL:       "test",
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		skipValidation:  true,
	}
	m, err := newMtsdb(context.Background(), nil, tstConfig)
	assert.NoError(err)

	go func() {
		for job := range m.job {
			testCntBulkLen.Add(uint64(job.Len()))
			m.wg.Done()
		}
	}()

	param := sync.Map{}
	one := &atomic.Uint64{}
	one.Add(1)
	param.Store("one", one)
	param.Store("two", one)
	param.Store("three", one)
	param.Store("four", one)
	m.insert(&param)
	m.wg.Wait()

	assert.Equal(uint64(4), testCntBulkLen.Load())

}

func TestFnvInsert(t *testing.T) {
	assert := require.New(t)
	testCntBulkLen := atomic.Uint64{}
	f := func() Hasher {
		return fnv.New32a()
	}
	tstConfig := Config{
		Size:            3,
		InsertSQL:       "test",
		Hasher:          f,
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		skipValidation:  true,
	}
	m, err := newMtsdb(context.Background(), nil, tstConfig)
	assert.NoError(err)

	go func() {
		for job := range m.job {
			testCntBulkLen.Add(uint64(job.Len()))
			m.wg.Done()
		}
	}()

	param := sync.Map{}
	one := &atomic.Uint64{}
	one.Add(1)
	param.Store("one", one)
	param.Store("two", one)
	param.Store("three", one)
	param.Store("four", one)
	m.insert(&param)
	m.wg.Wait()

	assert.Equal(uint64(4), testCntBulkLen.Load())

}
