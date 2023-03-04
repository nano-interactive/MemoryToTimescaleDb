package mtsdb

import (
	"sync/atomic"
)

func (m *Mtsdb) Inc(url string) {
	m.mu.Lock()
	if _, ok := m.container[url]; ok == false {
		m.container[url] = &atomic.Uint64{}
	}
	m.container[url].Add(1)
	if len(m.container) >= m.config.Size {
		go m.bulkInsert()
	}
	m.mu.Unlock()
}
