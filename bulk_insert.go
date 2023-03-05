package mtsdb

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"sync/atomic"
	"time"
)

func (m *Mtsdb) bulkInsert(async bool) {
	m.mu.Lock()
	insertContainer := m.container
	m.container = make(map[string]*atomic.Uint64)
	m.mu.Unlock()
	if async {
		go m.insert(insertContainer)
	} else {
		m.insert(insertContainer)
	}

}

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
	tm := time.Now().UnixMicro()
	m.wg.Add(1)
	defer m.wg.Done()
	fmt.Println("send")
	br := m.pool.SendBatch(m.ctx, batch)
	fmt.Println("send2")
	//execute statements in batch queue
	_, err := br.Exec()
	if err != nil {
		m.ChnErr <- err
	}
	err = br.Close()
	if err != nil {
		m.ChnErr <- err
	}
	fmt.Println("spent ", time.Now().UnixMicro()-tm, "len", batch.Len())
}
