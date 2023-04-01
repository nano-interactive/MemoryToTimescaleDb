package mtsdb

import "sync"

func (m *mtsdb) reset(resetCounter bool) *sync.Map {
	if resetCounter {
		m.containerLen.Store(0)
	}
	return m.container.Swap(&sync.Map{})
}
