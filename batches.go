package yadb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	clickhouse "github.com/undiabler/clickhouse-driver"
)

// BatchWriter write records into table in async way with batching
type BatchWriter struct {
	columns    clickhouse.Columns
	bulk_items int
	ticker     time.Duration
	table      string

	work      chan clickhouse.Row
	done      chan bool
	closeFlag *int32

	getConn func() clickhouse.Connector
}

// TODO: threadsafe workers map
var (
	workers = map[*BatchWriter]bool{}
	wg      sync.WaitGroup
)

// NewBatchWriter return BatchWriter with working goroutine inside
func NewBatchWriter(table string, columns []string, bulk_items int, ticker time.Duration) (*BatchWriter, error) {

	if bulk_items <= 1 {
		return nil, fmt.Errorf("Bulk must be greater than 1. Have %d", bulk_items)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("No columns for request")
	}

	bw := new(BatchWriter)
	bw.columns = columns
	bw.bulk_items = bulk_items
	bw.ticker = ticker
	bw.table = table
	bw.closeFlag = new(int32)
	atomic.StoreInt32(bw.closeFlag, 1)

	bw.work = make(chan clickhouse.Row, bulk_items)
	bw.done = make(chan bool)

	wg.Add(1)

	// TODO: this is unsafe
	workers[bw] = true

	go bw.worker(ticker, &wg)

	return bw, nil

}

// SetConn defines callback that should return Connector to run MultiInsert query
func (bw *BatchWriter) SetConn(f func() clickhouse.Connector) {
	bw.getConn = f
}

// IsClosed check if BatchWriter is closed
func (bw *BatchWriter) IsClosed() bool {
	return atomic.LoadInt32(bw.closeFlag) == 0
}

// Close close all chans, stop working goroutine.
func (bw *BatchWriter) Close() {
	if atomic.SwapInt32(bw.closeFlag, 0) == 1 {
		close(bw.work)
	}
}

// CloseAll stop any working BatchWriters. Safely wait for inserting all queued records.
func CloseAll() {

	log.Debug("Clickhouse goroutines exiting...")

	// close all registered workers
	for worker, _ := range workers {
		worker.Close()
	}

	// wait all goroutines to exit safely
	wg.Wait()
}
