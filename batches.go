package yadb

import (
	"errors"
	"fmt"
	"time"

	"github.com/roistat/go-clickhouse"
)

// write records into table in async way with batching
type BatchWriter struct {
	columns    clickhouse.Columns
	bulk_items int
	ticker     time.Duration

	work chan clickhouse.Row

	getConn func() *clickhouse.Conn
}

func NewBatchWriter(columns []string, bulk_items int, ticker time.Duration) (*BatchWriter, error) {

	if bulk_items <= 1 {
		return nil, fmt.Errorf("Bulk must be greater than 1. Have %d", bulk_items)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("No columns for request")
	}

	bw := new(BatchWriter)
	bw.columns = columns.(clickhouse.Columns)
	bw.bulk_items = bulk_items
	bw.ticker = ticker

	bw.work = make(chan clickhouse.Row, bulk_items)

	wg.Add(1)
	go bw.getObjects(obj_chan, ticker, wg)

	return bw, nil

}

func (bw *BatchWriter) SetConn(f func() *clickhouse.Conn) {
	bw.getConn = f
}

func newWriter(cls clickhouse.Columns, table string) (chan clickhouse.Row, error) {

	if len(workers) >= MAX_WORKERS {
		return nil, errors.New("Maximum yadb workers limit!")
	}

	tmp_chan := make(chan clickhouse.Row, BATCH_LEN*2)

	wg.Add(1)
	go getObject(tmp_chan, cls, table, &wg)

	workers <- tmp_chan

	return tmp_chan, nil
}
