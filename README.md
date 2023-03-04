# MemoryToTimeScaleDb
Stores data in memory and then bulk inserts to TimeScaleDb

## Usage
All you need to do is to initialize `Mtsdb` value and call `Inc(URL)` for each counter call

Example:
```go
        cfg := Config{
		Size:      50_000,
	}
	m := New(context.Background(), nil, cfg)
	
	m.Inc("https://example.com/hello")
```
