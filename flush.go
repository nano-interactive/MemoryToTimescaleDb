package mtsdb

func (m *mtsdb) Flush() {
	m.insert()
}
