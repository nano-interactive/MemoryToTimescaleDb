package mtsdb

func (m *Mtsdb) Flush() {
	m.bulkInsert()
}
