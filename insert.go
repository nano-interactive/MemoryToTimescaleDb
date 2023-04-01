package mtsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

func (m *mtsdb) insert(mapToInsert *sync.Map) {
	defer m.wg.Done()
	m.wg.Add(1)

	batch := new(pgx.Batch)

	mapToInsert.Range(func(key, value any) bool {
		if m.config.Hasher != nil {
			h := m.config.Hasher()
			_, err := h.Write([]byte(key.(string)))
			if err != nil {
				select { // non-blocking channel send
				case m.err <- err:
				default:
				}
			} else {
				batch.Queue(m.config.InsertSQL, h.Sum32(), value.(*atomic.Uint64).Load())
			}
		} else {
			batch.Queue(m.config.InsertSQL, key, value.(*atomic.Uint64).Load())
		}

		if batch.Len() >= m.config.BatchInsertSize {
			m.wg.Add(1) // m.wg.Done() is on sendBatch
			m.job <- *batch
			batch = &pgx.Batch{}
		}

		return true
	})

	if batch.Len() > 0 {
		m.wg.Add(1) // m.wg.Done() is on sendBatch
		m.job <- *batch
	}
}

// bulk insert
func (m *mtsdb) sendBatch(batch *pgx.Batch) {
	defer m.wg.Done()

	tm := time.Now().UnixMilli()
	br := m.pool.SendBatch(context.Background(), batch)
	defer func(br pgx.BatchResults) {
		err := br.Close()
		if err != nil {
			select { // non-blocking channel send
			case m.err <- err:
			default:
			}

		}
	}(br)
	_, err := br.Exec()
	if err != nil {
		select { // non-blocking channel send
		case m.err <- err:
		default:
		}
		return
	}
	m.MetricInserts.Add(uint64(batch.Len()))
	m.MetricDurationMs.Add(uint64(time.Now().UnixMilli() - tm))
}
