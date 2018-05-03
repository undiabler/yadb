package yadb

import (
	"github.com/mintance/go-clickhouse"
	log "github.com/sirupsen/logrus"

	"sync"
	"time"
)

const (
	FAIL_WRITES = 10
)

func (bw *BatchWriter) worker(tick time.Duration, done *sync.WaitGroup) {

	to_write := clickhouse.Rows{}
	tickChan := time.NewTicker(tick).C

	need_exit := false
	defer done.Done()

	for {

		need_write := false

		select {

		// regular getting items
		case item, ok := <-bw.work:

			if !ok {
				log.Debugf("CL goroutine %q exiting...", bw.table)
				need_exit = true
				for x := range bw.work {
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

				if conn := bw.getConn(); conn != nil {

					log.Debugf("Bulk inserting %d recs into %q (host: %s)", len(to_write), bw.table, conn.Host)

					if err := query.Exec(conn); err == nil {

						log.Debugf("Db %q: %d", bw.table, len(to_write))
						to_write = to_write[:0]
						break

					} else {
						log.Warningf("Bulk failed %d recs into %q (host: %s): %s", len(to_write), bw.table, conn.Host, err)
					}
				} else {
					log.Warningf("No active connections to db")
				}

				time.Sleep(time.Second)

			}

			if len(to_write) > 0 {
				log.Errorf("Too many fails, lost %d recs! Dump:%++v", len(to_write), to_write)
				to_write = to_write[:0]
			}

		}

		if need_exit {
			return
		}

	}

}
