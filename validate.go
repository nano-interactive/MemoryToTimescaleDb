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
	if config.TableName == "" {
		return errors.New("table name is empty")
	}
	if config.BatchInsertSize < 1 {
		return errors.New("batch insert size has to be > 0")
	}
	return nil
}
