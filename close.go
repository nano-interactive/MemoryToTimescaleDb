package mtsdb

func (m *Mtsdb) Close() error {
	m.cancel() // Stop the workers
	m.insert(m.reset(true))
	// wait for postgres batch to finish
	m.wg.Wait()
	close(m.err)
	return nil
}
