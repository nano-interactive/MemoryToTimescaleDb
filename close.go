package mtsdb

func Close() {
	bulkInsert()
	close(chnErr)
}
