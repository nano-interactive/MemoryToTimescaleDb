package mtsdb

import (
	"sync/atomic"
)

func (m *Mtsdb) Stats() (uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r1 := m.MetricInserts.Load()
	r2 := m.MetricDurationMs.Load()
	if r2 > 1e15 {
		m.MetricInserts = atomic.Uint64{}
		m.MetricDurationMs = atomic.Uint64{}
	}
	return r1, r2
}
