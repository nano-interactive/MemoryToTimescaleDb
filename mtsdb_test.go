package mtsdb

import (
	"context"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"hash/fnv"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	assert := require.New(t)

	insertInc := atomic.Uint64{}

	tstConfig := Config{
		Size:           5,
		InsertSQL:      "test",
		InsertDuration: 0,
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		insertInc.Add(uint64(batch.Len()))
	}

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
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		insertInc.Add(uint64(batch.Len()))
	}

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

	m := New(context.Background(), nil)
	assert.Equal(uint64(0), m.config.Size)
	_ = m.Close()

	m2 := New(context.Background(), nil, Config{Size: 100_000, InsertSQL: "test", InsertDuration: 2 * time.Minute})
	assert.Equal(uint64(100_000), m2.config.Size)
	assert.Equal(2*time.Minute, m2.config.InsertDuration)
	_ = m2.Close()

}

func TestPanic(t *testing.T) {
	assert := require.New(t)
	cfg := Config{
		Size:      0,
		InsertSQL: "",
	}

	assert.Panics(func() {
		New(context.Background(), nil, cfg)
	})

}

func BenchmarkAdd(b *testing.B) {
	b.ReportAllocs()

	gofakeit.Seed(100)
	urls := make([]string, 20_000)
	for i := 0; i < 20_000; i++ {
		urls[i] = gofakeit.URL()
	}

	tstConfig := Config{
		Size:      10_000,
		InsertSQL: "test",
	}

	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(*pgx.Batch) {}

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
		Size:      10_000,
		InsertSQL: "test",
		Hasher:    f,
	}

	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(*pgx.Batch) {}

	rnd := rand.New(rand.NewSource(100))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Inc(urls[rnd.Intn(10_000)])
	}

	_ = m.Close()
}
