package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"sync/atomic"
	"time"
)

type Mtsdb interface {
	Errors() <-chan error
	Inc(labels ...string)
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
	container atomic.Pointer[prometheus.CounterVec]
	labels    []string

	config Config

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
		pool:             pool,
		config:           config,
		ctx:              newCtx,
		cancel:           cancel,
		container:        atomic.Pointer[prometheus.CounterVec]{},
		err:              make(chan error, 100),
		job:              make(chan pgx.Batch, config.WorkerPoolSize),
		MetricInserts:    atomic.Uint64{},
		MetricDurationMs: atomic.Uint64{},
		labels:           labels,
	}
	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mtsdb",
	}, labels)
	m.container.Store(counterVec)

	// initialize worker pool
	for i := 0; i < config.WorkerPoolSize; i++ {
		go m.worker()
	}

	go m.startTicker(newCtx, config.InsertDuration)

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
			m.insert(m.reset())
		case <-ctx.Done():
			return
		}
	}
}
