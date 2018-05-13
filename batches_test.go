package yadb

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clickhouse "github.com/undiabler/clickhouse-driver"
)

var calls = 0

func TestBatches(t *testing.T) {

	var err error

	_, err = NewBatchWriter("events", []string{"uuid", "event_type", "event_time", "event_date"}, 1, time.Second)
	assert.Error(t, err)
	_, err = NewBatchWriter("events", []string{}, 5, time.Second)
	assert.Error(t, err)

}

func TestWriterErrors(t *testing.T) {

	tr := &badTransport{err: errors.New("Error inserting"), calls: 0}
	conn := clickhouse.NewConn("host.local", tr)

	bw, err := NewBatchWriter("events", []string{"uuid", "event_type", "event_time", "event_date"}, 5, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// skip insert
	bw.InsertMap(map[string]interface{}{
		"uuid":       "prog_test3",
		"event_type": 4,
		"event_date": time.Now().Format("2006-01-02"),
		"event_time": time.Now().Format("2006-01-02 15:04:05"),
	})
	time.Sleep(2 * time.Second)

	assert.Equal(t, 0, tr.calls)

	bw.SetConn(func() clickhouse.Connector {
		return nil
	})

	// no conn
	bw.InsertMap(map[string]interface{}{
		"uuid":       "prog_test3",
		"event_type": 4,
		"event_date": time.Now().Format("2006-01-02"),
		"event_time": time.Now().Format("2006-01-02 15:04:05"),
	})
	time.Sleep(11 * time.Second)

	assert.Equal(t, 0, tr.calls)

	bw.SetConn(func() clickhouse.Connector {
		return conn
	})

	bw.InsertMap(map[string]interface{}{
		"uuid":       "prog_test3",
		"event_type": 4,
		"event_date": time.Now().Format("2006-01-02"),
		"event_time": time.Now().Format("2006-01-02 15:04:05"),
	})

	time.Sleep(11 * time.Second)
	assert.Equal(t, 10, tr.calls)

	CloseAll()

	erre := bw.InsertMap(map[string]interface{}{
		"uuid":       "prog_test3",
		"event_type": 4,
		"event_date": time.Now().Format("2006-01-02"),
		"event_time": time.Now().Format("2006-01-02 15:04:05"),
	})

	assert.Error(t, erre)
}
