package mtsdb

import (
	"context"
	"fmt"
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
	err     chan error
	job     chan pgx.Batch
	ctx     context.Context
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	pool    PoolInterface
	metrics sync.Map

	config            Config
	concurrentInserts atomic.Int32

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
		metrics: sync.Map{},

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
	cnt := 0
	m.metrics.Range(func(key, value any) bool {
		cnt++
		return true
	})
	for i, metric := range metrics {
		m.metrics.Store(cnt+i, metric)
	}
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
			if m.concurrentInserts.Load() > 10 {
				m.err <- fmt.Errorf(
					"number of concurrent inserts into tmsdb %d, tick duration is not enough for all lines to be inserted",
					m.concurrentInserts.Load())
			}
			go m.insert()
		case <-ctx.Done():
			return
		}
	}
}
