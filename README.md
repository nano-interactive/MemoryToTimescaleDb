# MemoryToTimescaleDb
`Mtsdb` is in-memory counter that acts like caching layer. It stores `labels` as string and increments by 1 if the `Inc(labels...)` is called.
After predefined `InsertDuration` it bulk-inserts data into timescaledb.

In Nano, we use this library in real-time pre-bid stream to collect data for Online Marketing Planning Insights and Reach estimation. 

## Usage
Initialize MemoryToTimescaleDb `Mtsdb.New`, create and register `Metrics` and call `Inc(labels...)` for each incremental request or `Add(count,labels...)` for incrementing by `count` value.

Example:
```
    
    cg := mtsdb.Config{
		InsertDuration:  1000 * time.Millisecond,
		WorkerPoolSize:  5,
		BatchInsertSize: 1_000,
	}
	var err error
	m, err = mtsdb.New(context.Background(), dbpool, cg)
	if err != nil {
		panic(err)
	}

	c, err = mtsdb.NewMetricCounter(context.Background(), "cnt", mtsdb.MetricCounterConfig{
		TableName:   "url_prom_list",
		Description: "",
	}, "url", "country", "device")
	
	if err != nil {
		panic(err)
	}

	m.MustRegister(c)
	
    m.Inc("https://example.com/hello","RS","Mobile")
    // or 
    m.Add("https://example.com/hello","RS","Mobile")
```

### Config Mtsdb params

| Param           | Type          | Description                                                                                                                                                  |
|-----------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| InsertDuration  | time.Duration | Default: 1 * time.Minute. Tick duration that triggers the batch-insert                                                                                       |
| WorkerPoolSize  | int           | Default: 5. Maximum number of concurrent batch-inserts                                                                                                       | 
| BatchInsertSize | int           | Default: 1_000. The size of the batch for insert. For example if the `Size=10_000` and `BatchSizeInsert=1_000`, the worker will 10 times do the batch-insert |


### MetricCounterConfig

| Param           | Type   | Description                                                                                                                                                  |
|-----------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| TableName       | string | Name of the TimescaleDB table where the Metric data will be inserted                                                                                         |
| Description     | string | Description of the Metric                                                                                                                                    | 

### How to set up TimescaleDB Table
The metrics are inserted into TimescaleDb. When creating TimescaleDb table make sure to include `timestamp column` 
and `column name cnt int` as mandatory columns. Make sure that `timestamp` column has `CURRENT_TIMESTAMP` as default value.

For example, if you would like to track `url`,`country` and `device` from `mtsdb`:
```go
c, err = mtsdb.NewMetricCounter(context.Background(), "cnt", mtsdb.MetricCounterConfig{
    TableName:   "metrics",
    Description: "",
}, "url", "country", "device")
```
and the SQL:
```postgresql
CREATE TABLE IF NOT EXISTS metrics
(
    "time" timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    url text,
    country text,
    device text,
    cnt integer
);

SELECT create_hypertable('metrics','time');

CREATE INDEX idx_url ON metrics (url);
```