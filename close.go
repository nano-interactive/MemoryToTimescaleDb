package mtsdb

import "context"

func (m *Mtsdb) Close() error {
	m.cancel()      // Stop the workers
	close(m.closed) // Signal to the Inc that we are not accepting more data
	m.insert(context.Background(), m.reset(true))
	close(m.err)
	return nil
}
