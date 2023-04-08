package mtsdb

import (
	"sync"
)

func (m *mtsdb) reset() *sync.Map {
	m.containerLen.Store(0)
	return m.container.Swap(&sync.Map{})
}
