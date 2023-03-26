package mtsdb

func (m *Mtsdb) Close() error {
	m.cancel() // Stop the workers
	m.insert(m.reset(true))
	close(m.err)
	return nil
}
