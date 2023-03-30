# MemoryToTimescaleDb
`Mtsdb` is in-memory counter that acts like caching layer. It stores `string` as a key in maps and increments by 1 if the key is present.
After predefined `Size` or `Duration` it bulk-inserts data into timescaledb.

## Usage
Initialize MemoryToTimescaleDb `Mtsdb.New` and call `Inc(string)` for each incremental request

Example:
```
m:= mtsdb.New(context.Context, *pgxpool.Pool, ...mtsdb.Config)
	
m.Inc("https://example.com/hello")
```

Config is optional but highly recommended. Look at `config.go` file. If both `InsertDuration` and `Size` are set, the library will batch-insert what comes first.

Config params

| Param           | Type          | Description                                                                                                                                                  |
|-----------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Size            | uint64        | Memory item size before batch-insert occurs                                                                                                                  |
| InsertDuration  | time.Duration | Instead of using predefined `Size` for the batch-insert you can use predefined `Duration` for the batch-insert to happen                                     |
| InsertSQL       | string        | SQL statement for TimescaleDb, Default value: `INSERT INTO url_list (time,url,cnt) VALUES (now(),$1,$2)`                                                     |
| WorkerPoolSize  | int           | If you use `Size` for the bulk-insert, you need to specify maximum number of concurrent batch-inserts                                                        | 
| BatchInsertSize | int           | Default: 1_000. The size of the batch for insert. For example if the `Size=10_000` and `BatchSizeInsert=1_000`, the worker will 10 times do the batch-insert |
| Hasher          | func() Hasher | If you like to insert Hash value instead of `string` specify hash algorithm                                                                                  |

