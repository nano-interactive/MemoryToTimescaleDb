package mtsdb

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

// mockPool is a mock implementation of pgxpool.Pool.
type mockPool struct {
	mock.Mock
}

// SendBatch is a mocked method that returns a mock expectation.
func (m *mockPool) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	args := m.Called(ctx, batch)
	data := args.Get(0).(pgx.BatchResults)

	return data
}

// mockBatchResults is a mock implementation of pgx.BatchResults.
type mockBatchResults struct {
	mock.Mock
}

// Close is a mocked method of pgx.BatchResults.
func (m *mockBatchResults) Close() error {
	args := m.Called()
	err := args.Error(0)
	if args.Error(0) != nil {
		return err
	}
	return nil
}

func (m *mockBatchResults) Exec() (pgconn.CommandTag, error) {
	args := m.Called()
	err := args.Error(1)
	data := args.Get(0).(pgconn.CommandTag)

	if args.Error(1) != nil {
		return pgconn.CommandTag{}, err
	}

	return data, nil
}

func (m *mockBatchResults) Query() (pgx.Rows, error) {
	args := m.Called()
	err := args.Error(1)
	data := args.Get(0).(pgx.Rows)

	if args.Error(1) != nil {
		return data, err
	}

	return data, nil
}

func (m *mockBatchResults) QueryRow() pgx.Row {
	args := m.Called()
	data := args.Get(0).(pgx.Row)

	return data
}

func TestInsert(t *testing.T) {
	assert := require.New(t)

	mockPool := new(mockPool)
	mockBatchResults := new(mockBatchResults)

	// Set the expectations
	mockPool.On("SendBatch", mock.Anything, mock.Anything).Return(mockBatchResults).Times(2)
	mockBatchResults.On("Close").Return(nil).Times(2)
	mockBatchResults.On("Exec").Return(pgconn.CommandTag{}, nil).Times(2)

	tstConfig := Config{
		InsertDuration:  10 * time.Minute,
		WorkerPoolSize:  1,
		BatchInsertSize: 3,
	}
	ctx := context.Background()
	m, err := newMtsdb(ctx, mockPool, tstConfig)
	assert.NoError(err)

	c, err := NewMetricCounter(ctx, "testCounter", MetricCounterConfig{}, "label1")
	assert.NoError(err)

	m.MustRegister(c)

	c.Inc("one")
	c.Inc("two")
	c.Inc("three")
	c.Inc("four")
	c.Inc("three")
	c.Inc("four")

	m.Flush()
	m.wg.Wait()

	mockPool.AssertExpectations(t)
	mockBatchResults.AssertExpectations(t)
}

func TestInsertError(t *testing.T) {
	assert := require.New(t)

	mockPool := new(mockPool)
	mockBatchResults := new(mockBatchResults)

	// Set the expectations
	mockPool.On("SendBatch", mock.Anything, mock.Anything).Return(mockBatchResults).Times(2)
	mockBatchResults.On("Close").Return(nil).Times(2)
	mockBatchResults.On("Exec").Return(pgconn.CommandTag{}, errors.New("test")).Times(2)

	tstConfig := Config{
		InsertDuration:  10 * time.Minute,
		WorkerPoolSize:  1,
		BatchInsertSize: 3,
	}
	ctx := context.Background()
	m, err := newMtsdb(ctx, mockPool, tstConfig)
	assert.NoError(err)

	c, err := NewMetricCounter(ctx, "testCounter", MetricCounterConfig{}, "label1")
	assert.NoError(err)

	m.MustRegister(c)

	c.Inc("one")
	c.Inc("two")
	c.Inc("three")
	c.Inc("four")
	c.Inc("three")
	c.Inc("four")

	insertInc := atomic.Uint64{}
	go func() {
		for _ = range m.Errors() {
			insertInc.Add(1)
		}
	}()
	err = m.Close()
	assert.NoError(err)

	assert.Equal(uint64(2), insertInc.Load())

	mockPool.AssertExpectations(t)
	mockBatchResults.AssertExpectations(t)
}
