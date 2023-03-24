package mtsdb

import (
	"context"
	"sync/atomic"
)

func (m *Mtsdb) Inc(ctx context.Context, url string) {
	if url == "" {
		return
	}

	if m.config.Size != 0 && m.containerLen.CompareAndSwap(m.config.Size, 0) {
		old := m.reset(false)
		go m.insert(ctx, old)
	}

	value, loaded := m.container.Load().LoadOrStore(url, &atomic.Uint64{})

	if !loaded {
		m.containerLen.Add(1)
	}

	value.(*atomic.Uint64).Add(1)
}
