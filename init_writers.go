package yadb

import (
	"errors"
	"time"

	"github.com/roistat/go-clickhouse"
)

type BatchWriter struct {
	columns    clickhouse.Columns
	bulk_items int
	ticker     time.Duration

	getConn func() *clickhouse.Conn
}

func NewBatchWriter(columns []string, bulk_items int, ticker time.Duration) (*BatchWriter, error) {

	bw := new(BatchWriter)
	bw.columns = columns.(clickhouse.Columns)
	bw.bulk_items = bulk_items
	bw.ticker = ticker

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
