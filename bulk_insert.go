package mtsdb

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func (m *Mtsdb) insert(ctx context.Context, mapToInsert *sync.Map) {
	batch := new(pgx.Batch)

	mapToInsert.Range(func(key, value any) bool {
		if m.config.Hasher != nil {
			h := m.config.Hasher()
			_, err := h.Write([]byte(key.(string)))
			if err != nil {
				m.err <- err
			} else {
				batch.Queue(m.config.InsertSQL, h.Sum32(), value)
			}
		} else {
			batch.Queue(m.config.InsertSQL, key, value)
		}

		if batch.Len() >= 1_000 {
			m.bulk(ctx, batch)
			batch = &pgx.Batch{}
		}

		return true
	})

	if batch.Len() > 0 {
		m.bulk(ctx, batch)
	}
}

// bulk insert
func (m *Mtsdb) bulk(ctx context.Context, batch *pgx.Batch) {
	tm := time.Now().UnixMilli()
	br := m.pool.SendBatch(ctx, batch)
	_, err := br.Exec()
	if err != nil {
		m.err <- err
		return
	}
	defer func(br pgx.BatchResults) {
		err := br.Close()
		if err != nil {
			m.err <- err
		}
	}(br)
	m.MetricInserts.Add(uint64(batch.Len()))
	m.MetricDurationMs.Add(uint64(time.Now().UnixMilli() - tm))
}
