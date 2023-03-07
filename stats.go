package mtsdb

import "sync/atomic"

func (m *Mtsdb) Stats() (uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r1 := m.Inserts.Load()
	r2 := m.DurationMs.Load()
	if r2 > 1e15 {
		m.Inserts = atomic.Uint64{}
		m.DurationMs = atomic.Uint64{}
	}
	return r1, r2
}
