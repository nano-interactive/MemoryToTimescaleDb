package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestInsert(t *testing.T) {
	var testCntBulkLen int
	assert := require.New(t)

	tstConfig := Config{
		Size:      3,
		InsertSQL: "test",
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		testCntBulkLen += batch.Len()
	}

	param := sync.Map{}
	one := &atomic.Uint64{}
	one.Add(1)
	param.Store("one", one)
	param.Store("two", one)
	param.Store("three", one)
	param.Store("four", one)
	m.insert(&param)

	assert.Equal(4, testCntBulkLen)

}

func TestFnvInsert(t *testing.T) {
	assert := require.New(t)
	var testCntBulkLen int
	f := func() Hasher {
		return fnv.New32a()
	}
	tstConfig := Config{
		Size:      3,
		InsertSQL: "test",
		Hasher:    f,
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		testCntBulkLen += batch.Len()
	}

	param := sync.Map{}
	one := &atomic.Uint64{}
	one.Add(1)
	param.Store("one", one)
	param.Store("two", one)
	param.Store("three", one)
	param.Store("four", one)
	m.insert(&param)

	assert.Equal(4, testCntBulkLen)

}
