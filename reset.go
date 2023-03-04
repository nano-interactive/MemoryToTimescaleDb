package mtsdb

import "sync/atomic"

func Reset() {
	mu.Lock()
	container = make(map[string]*atomic.Uint64, containerSize)
	mu.Unlock()
}
