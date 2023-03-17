package mtsdb

func (m *Mtsdb) Inc(url string) {
	if url == "" {
		return
	}
	m.mu.Lock()
	if _, ok := m.container[url]; ok == false {
		m.container[url] = 0
	}
	isBulkInsert := m.config.Size != 0 && len(m.container) >= m.config.Size
	m.container[url]++
	m.mu.Unlock()
	if isBulkInsert {
		go m.bulkInsert()
	}
}
