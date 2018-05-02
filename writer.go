package yadb

import (
	"github.com/mintance/go-clickhouse"
	log "github.com/sirupsen/logrus"

	"fmt"
	"sync"
	"time"
)

var (
	CLICKHOUSE = ":8123"
)

const (
	FAIL_WRITES = 10
	MAX_WORKERS = 20
)

// TODO: теперь есть баг с ситуацией когда еще пишут в каналы а мы закрыли их!!
var (
	workers = map[*BatchWriter]bool{}
	wg      sync.WaitGroup
)

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

func (bw *BatchWriter) getObjects(obj_chan chan clickhouse.Row, tick time.Duration, done *sync.WaitGroup) {

	to_write := clickhouse.Rows{}
	tickChan := time.NewTicker(tick).C

	need_exit := false
	defer done.Done()

	for {

		need_write := false

		select {

		// regular getting items
		case item, ok := <-obj_chan:

			if !ok {
				log.Debugf("CL goroutine %q exiting...", bw.table)
				need_exit = true
				for x := range obj_chan {
					to_write = append(to_write, x)
				}
			} else {
				to_write = append(to_write, item)
			}

		// regular inserts on tick
		case <-tickChan:
			need_write = true

		}

		if len(to_write) >= bw.bulk_items {
			need_write = true
		}

		if (need_exit || need_write) && len(to_write) > 0 {

			if bw.getConn == nil {
				log.Warn("Skip db inserting!")
				to_write = to_write[:0]
				continue
			}

			query, err := clickhouse.BuildMultiInsert(bw.table,
				bw.columns,
				to_write,
			)

			if err != nil {
				log.Errorf("Build %q request fail: %s - %v", bw.table, err, to_write)
				continue
			}

			for i := 0; i < FAIL_WRITES; i++ {

				log.Debugf("Start writing (%d) objects to (%s)...", len(to_write), bw.table)

				if conn := bw.getConn(); conn != nil {

					if err := query.Exec(conn); err == nil {

						log.Debugf("Db %q: %d", bw.table, len(to_write))
						to_write = to_write[:0]
						break

					} else {
						log.Warningf("Error db %q: %s", bw.table, err)
					}
				} else {
					log.Warningf("No active connections to db")
				}

				time.Sleep(time.Second)

			}

			if len(to_write) > 0 {
				log.Errorf("Too many fails, ignore %d items! Dump:%++v", len(to_write), to_write)
				to_write = to_write[:0]
			}

		}

		if need_exit {
			return
		}

	}

}

func CloseAll() {

	log.Debug("Clickhouse goroutines exiting...")

	// close all registered workers
	for worker, _ := range workers {
		worker.Close()
	}

	// ждем пока все горутины допишут и выйдут
	wg.Wait()
}
