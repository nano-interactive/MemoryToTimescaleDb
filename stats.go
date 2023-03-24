package mtsdb

func (m *Mtsdb) Stats() (uint64, uint64) {
	if m.MetricDurationMs.CompareAndSwap(1e15, 0) {
		m.MetricInserts.Store(0)
	}

	return m.MetricInserts.Load(), m.MetricDurationMs.Load()
}
