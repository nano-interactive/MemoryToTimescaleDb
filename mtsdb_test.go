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
	assert := require.New(t)
	m, err := New(context.Background(), &pgxpool.Pool{}, CreateDefaultConfig())
	assert.NoError(err)
	assert.IsType(&mtsdb{}, m)
}

func TestNewMtsdb(t *testing.T) {
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		TableName:       "test",
		InsertDuration:  10 * time.Millisecond,
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
	m.Inc("six")
	m.Inc("four")
	m.Inc("six")
	m.Inc("six")
	m.Inc("six")
	m.Inc("three")
	m.Inc("six")
	m.Inc("six")

	checkOne, _ := m.fetchMetricValue("one")
	checkFour, _ := m.fetchMetricValue("four")
	checkSix, _ := m.fetchMetricValue("six")

	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")
	assert.Equal(float64(1), checkOne)
	assert.Equal(float64(2), checkFour)
	assert.Equal(float64(6), checkSix)

	_ = m.Close()
}

func TestTick(t *testing.T) {
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		TableName:       "test",
		InsertDuration:  10 * time.Millisecond,
		WorkerPoolSize:  0,
		BatchInsertSize: 1_000,
		skipValidation:  true,
	}
	m, err := newMtsdb(context.Background(), &pgxpool.Pool{}, tstConfig, "url")
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
	m.Inc("five")
	m.Inc("three")
	m.Inc("four")
	checkOne, _ := m.fetchMetricValue("one")
	checkFour, _ := m.fetchMetricValue("four")

	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")
	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")
	assert.Equal(float64(1), checkOne)
	assert.Equal(float64(2), checkFour)
	//
	time.Sleep(11 * time.Millisecond)

	_, err = m.fetchMetricValue("one")
	assert.ErrorIs(err, MetricNotFound)
	assert.Equal(uint64(5), insertInc.Load())

	m.Inc("six")
	m.Inc("six")
	checkSix, _ := m.fetchMetricValue("six")
	assert.Equal(float64(2), checkSix)

	_ = m.Close()
}

func TestInitConfig(t *testing.T) {
	assert := require.New(t)

	m, err := newMtsdb(context.Background(), &pgxpool.Pool{}, CreateDefaultConfig())
	assert.NoError(err)
	assert.Equal(5, m.config.WorkerPoolSize)
	assert.Equal(1_000, m.config.BatchInsertSize)
	_ = m.Close()

	m2, err := newMtsdb(context.Background(), &pgxpool.Pool{}, Config{
		TableName:       "test",
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
	assert := require.New(t)
	properCfg := CreateDefaultConfig()

	// nil pgxpool
	_, err := newMtsdb(context.Background(), nil, properCfg)
	assert.Error(err)

	// insertDuration 0
	cfg := properCfg
	cfg.InsertDuration = 0
	_, err = newMtsdb(context.Background(), &pgxpool.Pool{}, cfg)
	assert.Error(err)

	// empty table name
	cfg = properCfg
	cfg.TableName = ""
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
		TableName:       "test",
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		skipValidation:  true,
		InsertDuration:  5 * time.Minute,
	}

	m, err := newMtsdb(context.Background(), nil, tstConfig, "url")
	if err != nil {
		b.Error(err)
	}

	go func() {
		for range m.job {
			m.wg.Done()
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Inc(urls[rand.Intn(1000)])
		}
	})

	_ = m.Close()
}

//
//func BenchmarkFnvAdd(b *testing.B) {
//	b.ReportAllocs()
//
//	gofakeit.Seed(100)
//	urls := make([]string, 20_000)
//	for i := 0; i < 20_000; i++ {
//		urls[i] = gofakeit.URL()
//	}
//
//	f := func() Hasher {
//		return fnv.New32a()
//	}
//	tstConfig := Config{
//		Size:            10_000,
//		TableName:       "test",
//		WorkerPoolSize:  0,
//		BatchInsertSize: 1000,
//		skipValidation:  true,
//	}
//
//	m, err := newMtsdb(context.Background(), nil, tstConfig)
//	if err != nil {
//		b.Error(err)
//	}
//	go func() {
//		for range m.job {
//			m.wg.Done()
//		}
//	}()
//
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		for pb.Next() {
//			m.Inc(urls[rand.Intn(1000)])
//		}
//	})
//
//	_ = m.Close()
//}
