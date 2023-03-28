package mtsdb

func (m *Mtsdb) worker() {
	for job := range m.job {
		m.insert(job)
	}
}
