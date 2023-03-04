package mtsdb

import "sync/atomic"

func (m *Mtsdb) Reset() {
	m.mu.Lock()
	m.container = make(map[string]*atomic.Uint64, m.config.Size)
	m.mu.Unlock()
}
