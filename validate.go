package mtsdb

import (
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

func validate(pool *pgxpool.Pool, config Config) error {
	if config.skipValidation {
		return nil
	}
	if pool == nil {
		return errors.New("pgxpool can not be nil")
	}
	if config.InsertSQL == "" {
		return errors.New("insert sql is empty")
	}
	if config.Size == 0 && config.InsertDuration == 0 {
		return errors.New("set either Size of InsertDuration or both")
	}
	if config.Size > 0 {
		if config.WorkerPoolSize < 1 {
			return errors.New("worker pool size has to be > 0")
		}
	}
	if config.BatchInsertSize < 0 {
		return errors.New("batch insert size has to be > 0")
	}
	return nil
}
