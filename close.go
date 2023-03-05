package mtsdb

func (m *Mtsdb) Close() {
	m.bulkInsert(false)
	m.wg.Wait()
	close(m.ChnErr)
}
