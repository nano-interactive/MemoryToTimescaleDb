package mtsdb

func (m *Mtsdb) Flush() {
	m.insert(m.reset(true))
}
