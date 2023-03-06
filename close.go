package mtsdb

func (m *Mtsdb) Close() {
	m.bulkInsert()
	m.wg.Wait()
	close(m.ChnErr)
}
