package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"sync/atomic"
	"time"
)

type Mtsdb struct {
	ChnErr chan error

	config    Config
	container map[string]int
	mu        sync.Mutex
	wg        sync.WaitGroup
	bulkFunc  func(*pgx.Batch)
	pool      *pgxpool.Pool
	ctx       context.Context

	// stats
	Inserts    atomic.Uint64
	DurationMs atomic.Uint64
}

// New initialize maps and ticks, size has to be > 0
func New(ctx context.Context, pool *pgxpool.Pool, configMtsdb ...Config) *Mtsdb {
	var config Config
	if configMtsdb != nil {
		config = configMtsdb[0]
	} else {
		config = Config{
			Size: 100_000,
		}
	}

	if config.InsertSQL == "" {
		config.InsertSQL = "INSERT INTO url_list (time,url,cnt) VALUES (NOW(),$1,$2)"
	}

	m := &Mtsdb{
		ctx:        ctx,
		pool:       pool,
		config:     config,
		container:  make(map[string]int, config.Size),
		ChnErr:     make(chan error),
		Inserts:    atomic.Uint64{},
		DurationMs: atomic.Uint64{},
	}
	if config.InsertDuration > 0 {
		m.config.Size = 0
		go m.startTicker()
	} else if config.Size <= 0 {
		panic("mtsdb size has to be > 0")
	}
	m.bulkFunc = m.bulk

	return m
}

func (m *Mtsdb) startTicker() {
	ticker := time.NewTicker(m.config.InsertDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.bulkInsert()
		case <-m.ctx.Done():
			return
		}
	}
}
