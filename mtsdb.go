package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"hash"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

type Mtsdb interface {
	Errors() <-chan error
	Inc(labels ...string)
	IncBy(count uint32, labels ...string)
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
	labels    []string

	config       Config
	containerLen atomic.Uint64
	hash32       hash.Hash32

	// stats
	MetricInserts    atomic.Uint64
	MetricDurationMs atomic.Uint64
}

func New(ctx context.Context, pool *pgxpool.Pool, configMtsdb Config, labels ...string) (Mtsdb, error) {
	return newMtsdb(ctx, pool, configMtsdb, labels...)
}

func newMtsdb(ctx context.Context, pool *pgxpool.Pool, config Config, labels ...string) (*mtsdb, error) {

	err := validate(pool, config)
	if err != nil {
		return nil, err
	}

	newCtx, cancel := context.WithCancel(ctx)

	m := &mtsdb{
		pool:         pool,
		config:       config,
		hash32:       fnv.New32a(),
		ctx:          newCtx,
		cancel:       cancel,
		container:    atomic.Pointer[sync.Map]{},
		containerLen: atomic.Uint64{},

		err:              make(chan error, 100),
		job:              make(chan pgx.Batch, config.WorkerPoolSize),
		MetricInserts:    atomic.Uint64{},
		MetricDurationMs: atomic.Uint64{},
		labels:           labels,
	}

	m.container.Store(&sync.Map{})

	// initialize worker pool
	for i := 0; i < config.WorkerPoolSize; i++ {
		go m.worker()
	}

	if config.Size == 0 {
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
			if m.containerLen.Load() > 0 {
				m.insert(m.reset())
			}
		case <-ctx.Done():
			return
		}
	}
}
