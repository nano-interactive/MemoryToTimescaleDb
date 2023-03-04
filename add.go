package mtsdb

import "sync/atomic"

func Inc(url string) {
	mu.Lock()
	if container[url] == nil {
		container[url] = &atomic.Uint64{}
	}
	mu.Unlock()
	container[url].Add(1)
}
