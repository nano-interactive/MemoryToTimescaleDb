package mtsdb

import (
	"github.com/jackc/pgx/v5"
	"hash/fnv"
	"time"
)

func (m *Mtsdb) bulkInsert() {
	m.mu.Lock()
	insertContainer := m.container
	m.container = make(map[string]int)
	m.mu.Unlock()
	m.insert(insertContainer)
}

func (m *Mtsdb) insert(container map[string]int) {
	batch := &pgx.Batch{}
	for key, item := range container {
		if m.config.UseFnvHash {
			h := fnv.New32a()
			_, err := h.Write([]byte(key))
			if err != nil {
				m.ChnErr <- err
			} else {
				batch.Queue(m.config.InsertSQL, h.Sum32(), item)
			}
		} else {
			batch.Queue(m.config.InsertSQL, key, item)
		}
		if batch.Len() >= 1_000 {
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
	tm := time.Now().UnixMilli()
	m.wg.Add(1)
	defer m.wg.Done()
	br := m.pool.SendBatch(m.ctx, batch)
	//execute statements in batch queue
	_, err := br.Exec()
	if err != nil {
		m.ChnErr <- err
		return
	}
	err = br.Close()
	if err != nil {
		m.ChnErr <- err
		return
	}
	m.MetricInserts.Add(uint64(batch.Len()))
	m.MetricDurationMs.Add(uint64(time.Now().UnixMilli() - tm))
}
