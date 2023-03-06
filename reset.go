package mtsdb

func (m *Mtsdb) Reset() {
	m.mu.Lock()
	m.container = make(map[string]int, m.config.Size)
	m.mu.Unlock()
}
