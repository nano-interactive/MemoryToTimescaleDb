package mtsdb

import (
	"sync/atomic"
)

func (m *Mtsdb) Inc(url string) {
	if url == "" {
		return
	}

	if m.ctx.Err() != nil { // no more inserts
		return
	}

	value, loaded := m.container.Load().LoadOrStore(url, &atomic.Uint64{})
	if !loaded {
		m.containerLen.Add(1)
	}

	if m.config.Size != 0 && m.containerLen.CompareAndSwap(m.config.Size, 0) {
		old := m.reset(false)
		m.job <- old
	}

	value.(*atomic.Uint64).Add(1)
}
