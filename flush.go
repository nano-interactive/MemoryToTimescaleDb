package mtsdb

import "context"

func (m *Mtsdb) Flush(ctx context.Context) {
	m.insert(ctx, m.reset(true))
}
