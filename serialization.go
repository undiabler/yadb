package yadb

import (
	"fmt"
	"reflect"
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

func (bw *BatchWriter) InsertStruct(f interface{}) error {

	// TODO: try to convert to ToMap struct
	// TODO: seria call for getting map
	return nil
}
