package mtsdb

func (m *Mtsdb) Close() {
	m.bulkInsert()
	close(m.ChnErr)
}
