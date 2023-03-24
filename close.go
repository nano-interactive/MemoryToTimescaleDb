package mtsdb

import "context"

func (m *Mtsdb) Close() error {
	m.insert(context.Background(), m.reset(true))
	close(m.err)

	return nil
}
