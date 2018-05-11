package yadb

import (
	"fmt"
	"reflect"

	clickhouse "github.com/undiabler/clickhouse-driver"
	// log "github.com/sirupsen/logrus"
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

// InsertStruct get fields from struct using reflect and write them as map to DB
func (bw *BatchWriter) InsertStruct(f interface{}) error {

	// TODO: try to convert to ToMap struct
	// TODO: seria call for getting map
	return nil
}

// InsertMap get fields map and add them to BatchWriter queue
func (bw *BatchWriter) InsertMap(fields map[string]interface{}) error {

	if bw.IsClosed() {
		return fmt.Errorf("Goroutine cant accept data")
	}

	row := make(clickhouse.Row, 0, len(bw.columns))

	for _, column := range bw.columns {

		if tmp, ok := fields[column]; ok {
			row = append(row, tmp)
		} else {
			return fmt.Errorf("Missed column %q", column)
		}

	}

	bw.work <- row
	return nil
}
