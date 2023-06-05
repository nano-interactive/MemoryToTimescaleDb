package mtsdb

import (
	"errors"
)

func validate(pool PoolInterface, config Config) error {
	if config.skipValidation {
		return nil
	}

	if pool == nil {
		return errors.New("pgxpool can not be nil")
	}

	if config.WorkerPoolSize < 1 {
		return errors.New("worker pool size has to be > 0")
	}
	if config.BatchInsertSize < 1 {
		return errors.New("batch insert size has to be > 0")
	}
	if config.InsertDuration < 1 {
		return errors.New("insert duration or size has to be > 0")
	}
	return nil
}
