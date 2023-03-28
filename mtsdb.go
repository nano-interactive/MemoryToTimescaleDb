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
	err       chan error
	job       chan *sync.Map
	ctx       context.Context
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	pool      *pgxpool.Pool
	container atomic.Pointer[sync.Map]

	config       Config
	containerLen atomic.Uint64

	// bulk func
	bulkFunc func(*pgx.Batch)

	// stats
	MetricInserts    atomic.Uint64
	MetricDurationMs atomic.Uint64
}

var DefaultConfig = Config{
	Size:            0,
	Hasher:          nil,
	InsertDuration:  1 * time.Minute,
	InsertSQL:       "INSERT" + " INTO url_list (time,url,cnt) VALUES (now(),$1,$2)",
	WorkerPoolSize:  5,
	BatchInsertSize: 1_000,
}

// New initialize maps and ticks, size has to be > 0
func New(ctx context.Context, pool *pgxpool.Pool, configMtsdb ...Config) *Mtsdb {
	config := DefaultConfig

	if len(configMtsdb) > 0 {
		config = configMtsdb[0]

		if config.InsertSQL == "" {
			panic("insert sql is empty")
		}
		if config.Size > 0 {
			if config.WorkerPoolSize < 1 {
				panic("worker pool size has to be > 0")
			}
			if config.BatchInsertSize < 1 {
				panic("batch insert size has to be > 0")
			}
		}
	}

	newCtx, cancel := context.WithCancel(ctx)

	m := &Mtsdb{
		pool:             pool,
		config:           config,
		ctx:              newCtx,
		cancel:           cancel,
		container:        atomic.Pointer[sync.Map]{},
		err:              make(chan error, 0),
		job:              make(chan *sync.Map, config.WorkerPoolSize),
		containerLen:     atomic.Uint64{},
		MetricInserts:    atomic.Uint64{},
		MetricDurationMs: atomic.Uint64{},
	}
	m.container.Store(&sync.Map{})

	m.bulkFunc = m.bulk

	// initialize worker pool
	for i := 0; i < config.WorkerPoolSize; i++ {
		go m.worker()
	}

	if config.InsertDuration > 0 {
		go m.startTicker(newCtx, config.InsertDuration)
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
			m.insert(m.reset(true))
		case <-ctx.Done():
			return
		}
	}
}
