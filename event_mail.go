package yadb

import (
	"fmt"
	"reflect"

	"github.com/jbenet/go-base58"
	"github.com/roistat/go-clickhouse"
	// log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

/*
CREATE TABLE events (

	uuid String,

	c_run UInt16,

	event_type UInt8,

	event_time DateTime,
	event_date Date

) ENGINE = MergeTree(event_date, intHash32(c_run), (intHash32(c_run), event_type, cityHash64(uuid), event_date), 8192)
*/

func seria(f interface{}) {
	val := reflect.ValueOf(f).Elem()

	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		tag := typeField.Tag

		fmt.Printf("Field Name: %s,\t Field Value: %v,\t Tag Value: %s\n", typeField.Name, valueField.Interface(), tag.Get("tag_name"))
	}
}

type Counter struct {
	mu sync.Mutex
	x  int64
}

var counter Counter

func (c *Counter) inc() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.x += 1
	return c.x
}

type Log struct {
	uuid string

	// TODO: протестировать roistat/go-clickhouse, не работает адекватно с uint и тому подобными, только с int
	c_run int

	// private
	c_counter int64

	event_type int

	event_time string
	event_date string
}

func NewEvent(e_type uint8, run_counter int) *Log {

	e := &Log{}

	t := time.Now().UTC()

	e.event_type = int(e_type)

	e.event_date = t.Format("2006-01-02")
	e.event_time = t.Format("2006-01-02 15:04:05")

	e.c_run = run_counter
	e.c_counter = counter.inc()

	e.uuid = fmt.Sprintf("U%d-%d", e.c_run, e.c_counter)

	return e
}

func (l *Log) Send(to chan clickhouse.Row) {

	if l == nil {
		return
	}

	// log.Debugf("Elem : %#v", l)
	// return

	to <- clickhouse.Row{
		l.uuid,
		l.c_run,
		l.event_type,
		l.event_time,
		l.event_date,
	}
}

func (l *Log) GetHash() string {
	return base58.Encode([]byte(l.uuid))
}

func (l *Log) GetDomainKey() string {
	return subkey.EncodeKey(uint16(l.c_run), uint32(l.c_counter))
}
