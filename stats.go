package mtsdb

func (m *mtsdb) Stats() (uint64, uint64) {
	if m.MetricDurationMs.Load() > 1e15 {
		m.MetricInserts.Store(0)
		m.MetricDurationMs.Store(0)
	}

	return m.MetricInserts.Load(), m.MetricDurationMs.Load()
}
