package mtsdb

import (
	"context"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"hash/fnv"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	assert := require.New(t)
	m, err := New(context.Background(), &pgxpool.Pool{})
	assert.NoError(err)
	assert.IsType(&mtsdb{}, m)
}

func TestNewMtsdb(t *testing.T) {
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		Size:            5,
		InsertSQL:       "test",
		InsertDuration:  0,
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

	m.Inc("one")
	m.Inc("two")
	m.Inc("three")
	m.Inc("four")
	m.Inc("three")
	m.Inc("four")
	checkOne, _ := m.container.Load().Load("one")
	checkFour, _ := m.container.Load().Load("four")

	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")
	assert.Equal(uint64(1), checkOne.(*atomic.Uint64).Load())
	assert.Equal(uint64(2), checkFour.(*atomic.Uint64).Load())
	assert.Equal(uint64(4), m.containerLen.Load())

	m.Inc("five")
	time.Sleep(2 * time.Millisecond)

	assert.Equal(uint64(5), insertInc.Load())
	assert.Equal(uint64(0), m.containerLen.Load())

	m.Inc("six")
	m.Inc("six")
	checkSix, _ := m.container.Load().Load("six")
	assert.Equal(uint64(2), checkSix.(*atomic.Uint64).Load())

	_ = m.Close()
}

func TestTick(t *testing.T) {
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		Size:           0,
		InsertSQL:      "test",
		InsertDuration: 100 * time.Millisecond,
		WorkerPoolSize: 0,
		skipValidation: true,
	}
	m, err := newMtsdb(context.Background(), nil, tstConfig)
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
	checkOne, _ := m.container.Load().Load("one")
	checkFour, _ := m.container.Load().Load("four")
	assert.Equal(uint64(0), insertInc.Load(), "bulk insert should not be called")
	assert.Equal(uint64(1), checkOne.(*atomic.Uint64).Load())
	assert.Equal(uint64(2), checkFour.(*atomic.Uint64).Load())
	assert.Equal(uint64(5), m.containerLen.Load())
	//
	time.Sleep(110 * time.Millisecond)
	assert.Equal(uint64(5), insertInc.Load())
	assert.Equal(uint64(0), m.containerLen.Load())
	_, ok := m.container.Load().Load("one")
	assert.False(ok)
	m.Inc("six")
	m.Inc("six")
	checkSix, _ := m.container.Load().Load("six")
	assert.Equal(uint64(2), checkSix.(*atomic.Uint64).Load())

	_ = m.Close()
}

func TestInitConfig(t *testing.T) {
	assert := require.New(t)

	m, err := newMtsdb(context.Background(), &pgxpool.Pool{})
	assert.NoError(err)
	assert.Equal(uint64(0), m.config.Size)
	assert.Equal(5, m.config.WorkerPoolSize)
	assert.Equal(1_000, m.config.BatchInsertSize)
	_ = m.Close()

	m2, err := newMtsdb(context.Background(), nil, Config{
		Size:            100_000,
		InsertSQL:       "test",
		InsertDuration:  2 * time.Minute,
		WorkerPoolSize:  3,
		BatchInsertSize: 2_000,
		skipValidation:  true,
	})
	assert.NoError(err)
	assert.Equal(uint64(100_000), m2.config.Size)
	assert.Equal(2*time.Minute, m2.config.InsertDuration)
	assert.Equal(3, m2.config.WorkerPoolSize)
	assert.Equal(2_000, m2.config.BatchInsertSize)
	_ = m2.Close()

}

func TestErrors(t *testing.T) {
	assert := require.New(t)
	properCfg := Config{
		Size:            10_000,
		InsertSQL:       "test",
		WorkerPoolSize:  5,
		BatchInsertSize: 1_000,
	}

	// nil pgxpool
	_, err := newMtsdb(context.Background(), nil, properCfg)
	assert.Error(err)

	// size 0 and insertDuration 0
	cfg := properCfg
	cfg.Size = 0
	cfg.InsertDuration = 0
	_, err = newMtsdb(context.Background(), &pgxpool.Pool{}, cfg)
	assert.Error(err)

	// empty SQL
	cfg = properCfg
	cfg.InsertSQL = ""
	_, err = newMtsdb(context.Background(), &pgxpool.Pool{}, cfg)
	assert.Error(err)

	// batch insert size < 0
	cfg = properCfg
	cfg.BatchInsertSize = -1
	_, err = newMtsdb(context.Background(), &pgxpool.Pool{}, cfg)
	assert.Error(err)

	// worker pool size 0 with size > 0
	cfg = properCfg
	cfg.WorkerPoolSize = -1
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
		Size:            10_000,
		InsertSQL:       "test",
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		skipValidation:  true,
	}

	m, err := newMtsdb(context.Background(), nil, tstConfig)
	if err != nil {
		b.Error(err)
	}

	go func() {
		for range m.job {
			m.wg.Done()
		}
	}()

	rnd := rand.New(rand.NewSource(100))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Inc(urls[rnd.Intn(10_000)])
	}

	_ = m.Close()
}

func BenchmarkFnvAdd(b *testing.B) {
	b.ReportAllocs()

	gofakeit.Seed(100)
	urls := make([]string, 20_000)
	for i := 0; i < 20_000; i++ {
		urls[i] = gofakeit.URL()
	}

	f := func() Hasher {
		return fnv.New32a()
	}
	tstConfig := Config{
		Size:            10_000,
		InsertSQL:       "test",
		Hasher:          f,
		WorkerPoolSize:  0,
		BatchInsertSize: 1000,
		skipValidation:  true,
	}

	m, err := newMtsdb(context.Background(), nil, tstConfig)
	if err != nil {
		b.Error(err)
	}
	go func() {
		for range m.job {
			m.wg.Done()
		}
	}()

	rnd := rand.New(rand.NewSource(100))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Inc(urls[rnd.Intn(10_000)])
	}

	_ = m.Close()
}
