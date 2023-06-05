package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"sync"
	"sync/atomic"
	"time"
)

var _ Mtsdb = &mtsdb{}

type Mtsdb interface {
	Errors() <-chan error
	MustRegister(containers ...MetricInterface)
	Stats() (uint64, uint64)
	Close() error
}

type PoolInterface interface {
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
}

type metricContainer struct {
	atomic.Pointer[sync.Map]
}

type mtsdb struct {
	mu sync.Mutex

	err     chan error
	job     chan pgx.Batch
	ctx     context.Context
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	pool    PoolInterface
	metrics []MetricInterface

	config Config

	// stats
	MetricInserts    atomic.Uint64
	MetricDurationMs atomic.Uint64
}

func New(ctx context.Context, pool PoolInterface, configMtsdb Config) (Mtsdb, error) {
	return newMtsdb(ctx, pool, configMtsdb)
}

func newMtsdb(ctx context.Context, pool PoolInterface, config Config) (*mtsdb, error) {

	err := validate(pool, config)
	if err != nil {
		return nil, err
	}

	newCtx, cancel := context.WithCancel(ctx)

	m := &mtsdb{
		pool:    pool,
		config:  config,
		ctx:     newCtx,
		cancel:  cancel,
		metrics: make([]MetricInterface, 0),

		err:              make(chan error, 100),
		job:              make(chan pgx.Batch, config.WorkerPoolSize),
		MetricInserts:    atomic.Uint64{},
		MetricDurationMs: atomic.Uint64{},
	}

	// initialize worker pool
	for i := 0; i < config.WorkerPoolSize; i++ {
		go m.worker()
	}

	go m.startTicker(newCtx, config.InsertDuration)

	return m, nil
}

func (m *mtsdb) MustRegister(metrics ...MetricInterface) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics = append(m.metrics, metrics...)
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
			m.insert()
		case <-ctx.Done():
			return
		}
	}
}
