package mtsdb

import (
	"github.com/jackc/pgx/v5"
	"sync/atomic"
)

func (m *Mtsdb) bulkInsert() {
	m.mu.Lock()
	insertContainer := m.container
	m.container = make(map[string]*atomic.Uint64)
	m.mu.Unlock()
	go m.insert(insertContainer)

}

// gp:inline
func (m *Mtsdb) insert(container map[string]*atomic.Uint64) {
	batch := &pgx.Batch{}
	for key, item := range container {
		batch.Queue(m.config.InsertSQL, key, item.Load())
		if batch.Len() >= m.config.Size {
			m.bulkFunc(batch)
			batch = &pgx.Batch{}
		}
	}
	if batch.Len() > 0 {
		m.bulkFunc(batch)
	}
}

// bulk insert
func (m *Mtsdb) bulk(batch *pgx.Batch) {
	br := m.pool.SendBatch(m.ctx, batch)
	//execute statements in batch queue
	_, err := br.Exec()
	if err != nil {
		m.ChnErr <- err
	}
}
