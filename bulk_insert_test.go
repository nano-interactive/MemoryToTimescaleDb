package mtsdb

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func TestBulkInsert(t *testing.T) {

	ts := make(map[string]*uint64, 2)
	var a uint64 = 0
	ts["two"] = &a
	atomic.AddUint64(ts["two"], 1)

	fmt.Println(ts["two"])

	ts2 := ts

	ts = make(map[string]*uint64, 2)
	var b uint64 = 23
	ts["two"] = &b

	fmt.Println("ts", *ts["two"])
	fmt.Println("ts2", *ts2["two"])
}
func TestBulkInsert2(t *testing.T) {

	ts := make(map[string]*atomic.Uint64, 2)
	ts["two"] = &atomic.Uint64{}
	ts["two"].Add(1)

	fmt.Println(ts["two"].Load())

	ts2 := ts

	ts = make(map[string]*atomic.Uint64, 2)
	ts["two"] = &atomic.Uint64{}
	ts["two"].Add(23)

	fmt.Println("ts", ts["two"].Load())
	fmt.Println("ts2", ts2["two"].Load())
}
