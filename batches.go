package yadb

import (
	"fmt"
	"time"

	"github.com/mintance/go-clickhouse"
)

// write records into table in async way with batching
type BatchWriter struct {
	columns    clickhouse.Columns
	bulk_items int
	ticker     time.Duration
	table      string

	work chan clickhouse.Row

	getConn func() *clickhouse.Conn
}

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

	bw.work = make(chan clickhouse.Row, bulk_items)

	wg.Add(1)
	go bw.getObjects(bw.work, ticker, &wg)

	return bw, nil

}

func (bw *BatchWriter) SetConn(f func() *clickhouse.Conn) {
	bw.getConn = f
}
