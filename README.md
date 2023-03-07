# MemoryToTimeScaleDb
Stores data in memory and then bulk inserts to TimeScaleDb

## Usage
All you need to do is to initialize `Mtsdb` value and call `Inc(URL)` for each counter call

Example:
```
m:= mtsdb.New(context.Context, *pgxpool.Pool, ...mtsdb.Config)
	
m.Inc("https://example.com/hello")
```

Config is optional but highly recommended. Look at `config.go` file. If `InsertDuration` is set, the `Size` will be discarded.
