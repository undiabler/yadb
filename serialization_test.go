package yadb

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	ID       string `db:"uuid"`
	Type     int    `db:"event_type"`
	Date     string `db:"event_date"`
	DateTime string `db:"event_time"`
	EmptyVal string
	SkipVal  string `db:"-"`
}

func TestSeria(t *testing.T) {

	bw, err := NewBatchWriter("events", []string{"uuid", "event_type", "event_time", "event_date"}, 5, time.Second)

	assert.NoError(t, err)

	errw := bw.InsertMap(map[string]interface{}{
		"uuid":       "prog_test3",
		"event_date": time.Now().Format("2006-01-02"),
		"event_time": time.Now().Format("2006-01-02 15:04:05"),
	})

	assert.Error(t, errw)

	str1 := TestStruct{
		ID:       "prog_test3",
		Type:     4,
		Date:     time.Now().Format("2006-01-02"),
		DateTime: time.Now().Format("2006-01-02 15:04:05"),
	}

	map1 := map[string]interface{}{
		"uuid":       str1.ID,
		"event_type": str1.Type,
		"event_date": str1.Date,
		"event_time": str1.DateTime,
	}

	map2 := seria(reflect.ValueOf(&str1))
	assert.Equal(t, map1, map2)

	map3 := seria(reflect.ValueOf(str1))
	assert.Equal(t, map1, map3)

	errw2 := bw.InsertStruct(&str1)
	assert.NoError(t, errw2)

}
