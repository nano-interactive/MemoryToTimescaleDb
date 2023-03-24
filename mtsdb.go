package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"sync/atomic"
	"time"
)

type Mtsdb struct {
	err       chan error
	pool      *pgxpool.Pool
	container atomic.Pointer[sync.Map]

	config       Config
	containerLen atomic.Uint64

	// stats
	MetricInserts    atomic.Uint64
	MetricDurationMs atomic.Uint64
}

var DefaultConfig = Config{
	Size:           100_000,
	Hasher:         nil,
	InsertDuration: 1 * time.Minute,
	InsertSQL:      "INSERT INTO url_list (time,url,cnt) VALUES (now(),$1,$2)",
}

// New initialize maps and ticks, size has to be > 0
func New(ctx context.Context, pool *pgxpool.Pool, configMtsdb ...Config) *Mtsdb {
	config := DefaultConfig

	if len(configMtsdb) > 0 {
		config = configMtsdb[0]

		if config.InsertSQL == "" {
			panic("insert sql is empty")
		}
	}

	m := &Mtsdb{
		pool:             pool,
		config:           config,
		container:        atomic.Pointer[sync.Map]{},
		err:              make(chan error, 100),
		containerLen:     atomic.Uint64{},
		MetricInserts:    atomic.Uint64{},
		MetricDurationMs: atomic.Uint64{},
	}

	if config.InsertDuration > 0 {
		go m.startTicker(ctx, config.InsertDuration)
	}

	return m
}

func (m *Mtsdb) Errors() <-chan error {
	return m.err
}

func (m *Mtsdb) startTicker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.insert(ctx, m.reset(true))
		case <-ctx.Done():
			m.insert(context.Background(), m.reset(true))
			return
		}
	}
}
