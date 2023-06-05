package mtsdb

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type insertMetric struct {
	TableName string
	Container *sync.Map
	Labels    []string
}

func (m *mtsdb) insert() {
	m.wg.Wait()

	defer m.wg.Done()
	m.wg.Add(1)

	//m.mu.Lock()
	//defer m.mu.Unlock()

	for _, metric := range m.metrics {
		batch := new(pgx.Batch)

		im := metric.Write()
		sql := m.generateSql(im.TableName, im.Labels)
		im.Container.Range(func(key, value any) bool {
			values := make([]any, len(im.Labels)+1)
			for i, fieldValue := range value.(*MetricLabelValues).fields {
				values[i] = fieldValue
			}
			values[len(im.Labels)] = value.(*MetricLabelValues).count.Load()
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
		m.raiseError(err)
		return
	}
	m.MetricInserts.Add(uint64(batch.Len()))
	m.MetricDurationMs.Add(uint64(time.Now().UnixMilli() - tm))
}
