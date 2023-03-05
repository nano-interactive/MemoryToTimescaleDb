package mtsdb

import (
	"sync/atomic"
)

func (m *Mtsdb) Inc(url string) {
	m.mu.Lock()
	if _, ok := m.container[url]; ok == false {
		m.container[url] = &atomic.Uint64{}
	}
	isBulkInsert := len(m.container) >= m.config.Size
	m.mu.Unlock()
	m.container[url].Add(1)
	if isBulkInsert {
		m.bulkInsert(true)
	}
}
