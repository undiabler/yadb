package yadb

import (
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clickhouse "github.com/undiabler/clickhouse-driver"
)

var calls = 0

type mockTransport struct {
	response string
}

type badTransport struct {
	response string
	err      error
}

func (m mockTransport) Exec(host, params string, q clickhouse.Query, readOnly bool) (r string, err error) {
	calls++
	return m.response, nil
}

func (m badTransport) Exec(host, params string, q clickhouse.Query, readOnly bool) (r string, err error) {
	return "", m.err
}

func TestWriter(t *testing.T) {

	tr := mockTransport{response: "Ok."}
	conn := clickhouse.NewConn("host.local", tr)
	/*
		CREATE TABLE events (
			uuid       String,
			event_type UInt8,
			event_time DateTime,
			event_date Date
		) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/hits', '{replica}', event_date, intHash32(uuid), (intHash32(uuid), event_type, event_date), 8192)
	*/

	events, err := NewBatchWriter("events", []string{"uuid", "event_type", "event_time", "event_date"}, 5, time.Second)
	if err != nil {
		log.Fatalf("Cannot create writer: %s", err)
	}
	events.SetConn(func() clickhouse.Connector {
		return conn
	})
	time.Sleep(time.Second)

	for i := 0; i < 5; i++ {
		events.InsertMap(map[string]interface{}{
			"uuid":       "prog_test3",
			"event_type": 3,
			"event_date": time.Now().Format("2006-01-02"),
			"event_time": time.Now().Format("2006-01-02 15:04:05"),
		})
	}

	time.Sleep(time.Second)
	assert.Equal(t, 1, calls)
	time.Sleep(time.Second)

	events.InsertMap(map[string]interface{}{
		"uuid":       "prog_test3",
		"event_type": 4,
		"event_date": time.Now().Format("2006-01-02"),
		"event_time": time.Now().Format("2006-01-02 15:04:05"),
	})

	time.Sleep(time.Second)
	assert.Equal(t, 2, calls)

	events.InsertMap(map[string]interface{}{
		"uuid":       "prog_test3",
		"event_type": 4,
		"event_date": time.Now().Format("2006-01-02"),
		"event_time": time.Now().Format("2006-01-02 15:04:05"),
	})

	CloseAll()
	assert.Equal(t, 3, calls)
}
