package mtsdb

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync/atomic"
)

func bulkInsert() {
	mu.Lock()
	insertContainer := container
	container = make(map[string]*atomic.Uint64)
	mu.Unlock()
	insertFunc(context.Background(), nil, insertContainer)

}

// gp:inline
func insert(ctx context.Context, pool *pgxpool.Pool, container map[string]*atomic.Uint64) {
	batch := &pgx.Batch{}
	for key, item := range container {
		batch.Queue(config.InsertSQL, key, item.Load())
		if batch.Len() >= config.Size {
			bulk(ctx, pool, batch)
			batch = &pgx.Batch{}
		}
	}
	if batch.Len() > 0 {
		bulk(ctx, pool, batch)
	}
}

// bulk insert
func bulk(ctx context.Context, pool *pgxpool.Pool, batch *pgx.Batch) {
	br := pool.SendBatch(ctx, batch)
	//execute statements in batch queue
	_, err := br.Exec()
	if err != nil {
		chnErr <- err
	}
}
