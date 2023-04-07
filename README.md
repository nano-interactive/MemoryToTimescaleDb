# MemoryToTimescaleDb
`Mtsdb` is in-memory counter that uses acts like caching layer. It stores `labels` as string and increments by 1 if the `Inc(labels...)` is called.
After predefined `InsertDuration` it bulk-inserts data into timescaledb.

We use it to collect metrics of `data request` from our `LIIFTEngine` service.

## Usage
Initialize MemoryToTimescaleDb `Mtsdb.New` and call `Inc(labels...)` for each incremental request

Example:
```
m:= mtsdb.New(context.Context, *pgxpool.Pool, mtsdb.DefaultConfig(), "url","country")
	
m.Inc("https://example.com/hello","RS",)
```

Config params

| Param           | Type          | Description                                                                                                                                                                                                                                          |
|-----------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| InsertDuration  | time.Duration | Instead of using predefined `Size` for the batch-insert you can use predefined `Duration` for the batch-insert to happen                                                                                                                             |
| TableName       | string        | SQL table name for TimescaleDb, Default value: `url_prom_list`. Depending on number of labels it will generate SQL insert statement. For example if you have labels: one,two, it will generate: `INSERT INTO url_prom_list (one,two) VALUES ($1,$2)` |
| WorkerPoolSize  | int           | If you use `Size` for the bulk-insert, you need to specify maximum number of concurrent batch-inserts                                                                                                                                                | 
| BatchInsertSize | int           | Default: 1_000. The size of the batch for insert. For example if the `Size=10_000` and `BatchSizeInsert=1_000`, the worker will 10 times do the batch-insert                                                                                         |

