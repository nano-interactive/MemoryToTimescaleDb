package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"sync/atomic"
)

type Config struct {
	Size      int
	InsertSQL string
}

type Mtsdb struct {
	ChnErr chan error

	config    Config
	container map[string]*atomic.Uint64
	mu        sync.Mutex
	bulkFunc  func(*pgx.Batch)
	pool      *pgxpool.Pool
	ctx       context.Context
}

// New initialize maps and ticks, size has to be > 0
func New(ctx context.Context, pool *pgxpool.Pool, configMtsdb ...Config) *Mtsdb {
	var config Config
	if configMtsdb != nil {
		config = configMtsdb[0]
	} else {
		config = Config{
			Size:      100_000,
			InsertSQL: "INSERT INTO urls (url,cnt) VALUES ($1,$2)",
		}
	}
	if config.Size <= 0 {
		panic("mtsdb size has to be > 0")
	}

	m := &Mtsdb{
		ctx:       ctx,
		pool:      pool,
		config:    config,
		container: make(map[string]*atomic.Uint64, config.Size),
		ChnErr:    make(chan error),
	}
	m.bulkFunc = m.bulk

	return m
}
