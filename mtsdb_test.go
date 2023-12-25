package mtsdb

import (
	"context"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	m, err := New(context.Background(), &pgxpool.Pool{}, DefaultConfig())
	assert.NoError(err)
	assert.IsType(&mtsdb{}, m)
}

func TestNewMtsdb(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		InsertDuration:  10 * time.Millisecond,
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

	_ = c.Inc("one")
	_ = c.Inc("two")
	_ = c.Inc("four")
	_ = c.Inc("three")
	_ = c.Inc("four")
	checkOne, ok := c.Get("one")
	assert.True(ok)
	checkFour, _ := c.Get("four")
	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")
	assert.Equal(uint32(1), checkOne)
	assert.Equal(uint32(2), checkFour)

	m.insert()

	_ = c.Inc("one")
	_ = c.Inc("one")
	_ = c.Inc("two")
	_ = c.Inc("two")
	_ = c.Inc("two")
	_ = c.Inc("three")
	_ = c.Inc("one")
	_ = c.Inc("one")

	m.wg.Wait()

	checkOne, ok = c.Get("one")
	assert.True(ok)
	assert.Equal(uint32(4), checkOne)

	_, ok = c.Get("four")
	assert.False(ok)

	_ = m.Close()
}

func TestInitConfig(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	m, err := newMtsdb(context.Background(), &pgxpool.Pool{}, DefaultConfig())
	assert.NoError(err)
	assert.Equal(5, m.config.WorkerPoolSize)
	assert.Equal(1_000, m.config.BatchInsertSize)
	_ = m.Close()

	m2, err := newMtsdb(context.Background(), &pgxpool.Pool{}, Config{
		InsertDuration:  2 * time.Minute,
		WorkerPoolSize:  3,
		BatchInsertSize: 2_000,
	})
	assert.NoError(err)
	assert.Equal(2*time.Minute, m2.config.InsertDuration)
	assert.Equal(3, m2.config.WorkerPoolSize)
	assert.Equal(2_000, m2.config.BatchInsertSize)
	_ = m2.Close()

}

func TestErrors(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	properCfg := DefaultConfig()

	// nil pgxpool
	_, err := newMtsdb(context.Background(), nil, properCfg)
	assert.Error(err)

	// insertDuration 0
	cfg := properCfg
	cfg.InsertDuration = 0
	_, err = newMtsdb(context.Background(), &pgxpool.Pool{}, cfg)
	assert.Error(err)

	// batch insert size = 0
	cfg = properCfg
	cfg.BatchInsertSize = 0
	_, err = newMtsdb(context.Background(), &pgxpool.Pool{}, cfg)
	assert.Error(err)

	// worker pool size 0
	cfg = properCfg
	cfg.WorkerPoolSize = 0
	_, err = newMtsdb(context.Background(), &pgxpool.Pool{}, cfg)
	assert.Error(err)

}
func BenchmarkAdd(b *testing.B) {
	b.ReportAllocs()

	gofakeit.Seed(100)
	urls := make([]string, 20_000)
	for i := 0; i < 20_000; i++ {
		urls[i] = gofakeit.URL()
	}

	tstConfig := Config{
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		skipValidation:  true,
		InsertDuration:  5 * time.Minute,
	}

	ctx := context.Background()
	m, err := newMtsdb(ctx, nil, tstConfig)
	if err != nil {
		b.Error(err)
	}

	c, err := NewMetricCounter(ctx, "testCounter", MetricCounterConfig{}, "label1")
	if err != nil {
		b.Error(err)
	}

	m.MustRegister(c)

	go func() {
		for range m.job {
			m.wg.Done()
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.Inc(urls[rand.Intn(1000)])
		}
	})

	_ = m.Close()
}
