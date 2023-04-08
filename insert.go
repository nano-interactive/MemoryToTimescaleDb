package mtsdb

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func (m *mtsdb) insert(mapToInsert *sync.Map) {
	defer m.wg.Done()
	m.wg.Add(1)

	batch := new(pgx.Batch)

	sql := m.generateSql()
	mapToInsert.Range(func(key, value any) bool {
		values := make([]any, len(m.labels)+1)
		for i, fieldValue := range value.(*Metric).fields {
			values[i] = fieldValue
		}
		values[len(m.labels)] = value.(*Metric).count.Load()
		batch.Queue(sql, values...)

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

func (m *mtsdb) raiseError(err error) {
	select { // non-blocking channel send
	case m.err <- err:
	default:
	}

}

// bulk insert
func (m *mtsdb) sendBatch(batch *pgx.Batch) {
	defer m.wg.Done()

	tm := time.Now().UnixMilli()
	br := m.pool.SendBatch(context.Background(), batch)
	defer func(br pgx.BatchResults) {
		_ = br.Close()
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
