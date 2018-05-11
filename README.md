[![Travis status](https://img.shields.io/travis/undiabler/yadb.svg)](https://travis-ci.org/undiabler/yadb) 
[![GoDoc](https://godoc.org/github.com/undiabler/yadb?status.svg)](https://godoc.org/github.com/undiabler/yadb)
[![Go Report](https://goreportcard.com/badge/github.com/undiabler/yadb)](https://goreportcard.com/report/github.com/undiabler/yadb) 
[![Coverage Status](https://img.shields.io/coveralls/undiabler/yadb.svg)](https://coveralls.io/github/undiabler/yadb) 
![](https://img.shields.io/github/license/undiabler/clickhouse-driver.svg)

# yadb
Golang [Yandex ClickHouse](https://clickhouse.yandex/) bulk wrapper for [clickhouse driver](https://github.com/undiabler/clickhouse-driver/)

### About 
Clickhouse reccomends to insert data using large batches.  
Also using replication tables with 1 rec per insert will make you requests slower.  
This package helps to group incoming data on fly. It also controlls pauses to prevent long stacking data.

### Example 

```go
package main

import (
	"time"

	log "github.com/sirupsen/logrus"
	clickhouse "github.com/undiabler/clickhouse-driver"
	"github.com/undiabler/yadb"
)

func main() {
	httpTransport := clickhouse.NewTransport()
	conn := clickhouse.NewConn("host1", httpTransport)

	// example with replicating table, you can use any you want
	/*
		CREATE TABLE events (
			uuid       String,
			event_type UInt8,
			event_time DateTime,
			event_date Date
		) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/hits', '{replica}', 
		event_date, intHash32(uuid), (intHash32(uuid), event_type, event_date), 8192)
	*/

	events, err := yadb.NewBatchWriter(
		// table to write
		"events", 
		// columns for this table
		[]string{"uuid", "event_type", "event_time", "event_date"},
		// group items by batches from 5 elements 
		5, 
		// every 10 seconds write batch even if there are less than 5 elems
		10*time.Second
	)

	if err != nil {
		log.Fatalf("Cannot create writer: %s", err)
	}

	// you mush initialize function that return clickhouse connection
	// it's very usefull if you use clickhouse cluster, if no just pass you conn as in example
	events.SetConn(func() clickhouse.Connector {
		return conn
	})

	// write item to db, case error if some fields are missed
	events.InsertMap(map[string]interface{}{
		"uuid":       "progssl_test3",
		"event_type": 3,
		"event_date": time.Now().Format("2006-01-02"),
		"event_time": time.Now().Format("2006-01-02 15:04:05"),
	})

	// just sleep to show that previos record will be written after 10 sec
	time.Sleep(11 * time.Second)

	// new record
	events.InsertMap(map[string]interface{}{
		"uuid":       "progssl_test3",
		"event_type": 4,
		"event_date": time.Now().Format("2006-01-02"),
		"event_time": time.Now().Format("2006-01-02 15:04:05"),
	})

	// Gracefull exit. Stop waiting for 10 sec ticker, insert all records in queue and wait for it
	yadb.CloseAll()

}
```

### TODO

- [X] Inserting
- [x] Gracefull exit
- [ ] Struct mapping
- [x] Tests
- [x] Travis/Coverage/Goreport
- [ ] Unlimited queue
- [ ] Mem limited queue