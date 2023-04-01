package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"sync/atomic"
	"time"
)

type Mtsdb interface {
	Errors() <-chan error
	Inc(url string)
	Stats() (uint64, uint64)
	Close() error
}

type mtsdb struct {
	err       chan error
	job       chan pgx.Batch
	ctx       context.Context
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	pool      *pgxpool.Pool
	container atomic.Pointer[sync.Map]

	config       Config
	containerLen atomic.Uint64

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

func New(ctx context.Context, pool *pgxpool.Pool, configMtsdb ...Config) (Mtsdb, error) {
	return newMtsdb(ctx, pool, configMtsdb...)
}

func newMtsdb(ctx context.Context, pool *pgxpool.Pool, configMtsdb ...Config) (*mtsdb, error) {
	config := DefaultConfig

	if len(configMtsdb) > 0 {
		config = configMtsdb[0]
	}
	err := validate(pool, config)
	if err != nil {
		return nil, err
	}

	if config.BatchInsertSize == 0 {
		config.BatchInsertSize = 1_000
	}

	newCtx, cancel := context.WithCancel(ctx)

	m := &mtsdb{
		pool:             pool,
		config:           config,
		ctx:              newCtx,
		cancel:           cancel,
		container:        atomic.Pointer[sync.Map]{},
		err:              make(chan error, 100),
		job:              make(chan pgx.Batch, config.WorkerPoolSize),
		containerLen:     atomic.Uint64{},
		MetricInserts:    atomic.Uint64{},
		MetricDurationMs: atomic.Uint64{},
	}
	m.container.Store(&sync.Map{})

	// initialize worker pool
	for i := 0; i < config.WorkerPoolSize; i++ {
		go m.worker()
	}

	if config.InsertDuration > 0 {
		go m.startTicker(newCtx, config.InsertDuration)
	}

	return m, nil
}

func (m *mtsdb) Errors() <-chan error {
	return m.err
}

func (m *mtsdb) startTicker(ctx context.Context, interval time.Duration) {
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
