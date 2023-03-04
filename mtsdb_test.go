package mtsdb

import (
	"context"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	assert := require.New(t)

	var mu sync.Mutex

	insertInc := 0

	tstConfig := Config{
		Size:      5,
		InsertSQL: "",
	}
	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {
		mu.Lock()
		defer mu.Unlock()
		insertInc += batch.Len()
	}

	m.Inc("one")
	m.Inc("two")
	m.Inc("three")
	m.Inc("four")
	m.Inc("three")
	m.Inc("four")
	assert.Equal(0, insertInc, "bulk insert should not be called")
	assert.Equal(uint64(1), m.container["one"].Load())
	assert.Equal(uint64(2), m.container["four"].Load())

	m.Inc("five")
	time.Sleep(2 * time.Millisecond)
	mu.Lock()
	assert.Equal(5, insertInc)
	mu.Unlock()

	m.mu.Lock()
	assert.Equal(0, len(m.container))
	m.mu.Unlock()

	m.Inc("six")
	m.Inc("six")
	assert.Equal(uint64(2), m.container["six"].Load())

	m.Close()
}

func TestInitConfig(t *testing.T) {
	assert := require.New(t)

	m := New(context.Background(), nil)

	assert.Equal(100_000, m.config.Size)

	m.Close()
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
	b.ResetTimer()

	gofakeit.Seed(100)
	urls := make([]string, 20_000)
	for i := 0; i < 20_000; i++ {
		urls[i] = gofakeit.URL()
	}

	tstConfig := Config{
		Size:      10_000,
		InsertSQL: "",
	}

	m := New(context.Background(), nil, tstConfig)
	m.bulkFunc = func(batch *pgx.Batch) {}

	rnd := rand.New(rand.NewSource(100))

	for i := 0; i < b.N; i++ {
		m.Inc(urls[rnd.Intn(10_000)])
	}

	m.Close()
}
