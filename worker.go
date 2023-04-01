package mtsdb

func (m *mtsdb) worker() {
	for job := range m.job {
		m.sendBatch(&job)
	}
}
