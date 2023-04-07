package mtsdb

func (m *mtsdb) Close() error {
	m.cancel() // Stop the workers
	m.insert(m.reset())
	// wait for postgres batch to finish
	m.wg.Wait()
	close(m.err)
	close(m.job)
	return nil
}
