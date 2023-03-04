package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"sync/atomic"
	"time"
)

var (
	container     map[string]*atomic.Uint64
	mu            sync.Mutex
	insertFunc    func(context.Context, *pgxpool.Pool, map[string]*atomic.Uint64)
	chnErr        chan error
	containerSize int
	config        Config
)

type Config struct {
	Size         int
	InsertSQL    string
	BulkInterval time.Duration
}

// Init initialize maps and ticks, size has to be > 0 , if the bulkInsertInterval = 0 it will be skipped
func Init(ctx context.Context, configMtsdb ...Config) <-chan error {
	if configMtsdb == nil {
		config = configMtsdb[0]
	} else {
		config = Config{
			Size:         100_000,
			InsertSQL:    "INSERT INTO urls (url,cnt) VALUES ($1,$2)",
			BulkInterval: time.Minute,
		}
	}
	container = make(map[string]*atomic.Uint64, config.Size)
	insertFunc = insert
	chnErr = make(chan error)

	if config.Size <= 0 {
		panic("mtsdb size has to be > 0")
	}

	if config.BulkInterval > 0 {
		go startTicker(ctx)
	}
	return chnErr
}

func startTicker(ctx context.Context) {
	ticker := time.NewTicker(config.BulkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			bulkInsert()
		case <-ctx.Done():
			return
		}
	}
}
