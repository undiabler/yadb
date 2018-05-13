package yadb

import (
	log "github.com/sirupsen/logrus"
	clickhouse "github.com/undiabler/clickhouse-driver"

	"sync"
	"time"
)

const (
	// FAIL_WRITES number of tries to get new connection and exec query
	FAIL_WRITES = 10
)

func (bw *BatchWriter) worker(tick time.Duration, done *sync.WaitGroup) {

	toWrite := clickhouse.Rows{}
	tickChan := time.NewTicker(tick).C

	needExit := false
	defer done.Done()

	for {

		needWrite := false

		select {

		// regular getting items
		case item, ok := <-bw.work:

			if !ok {
				log.Debugf("CL goroutine %q exiting...", bw.table)
				needExit = true
				for x := range bw.work {
					toWrite = append(toWrite, x)
				}
			} else {
				toWrite = append(toWrite, item)
			}

		// regular inserts on tick
		case <-tickChan:
			needWrite = true

		}

		if len(toWrite) >= bw.bulkItems {
			needWrite = true
		}

		if (needExit || needWrite) && len(toWrite) > 0 {

			if bw.getConn == nil {
				log.Warn("Skip db inserting!")
				toWrite = toWrite[:0]
				continue
			}

			query, err := clickhouse.BuildMultiInsert(bw.table,
				bw.columns,
				toWrite,
			)

			log.Debugf("Query: %s", query)

			if err != nil {
				log.Errorf("Build %q request fail: %s - %v", bw.table, err, toWrite)
				continue
			}

			for i := 0; i < FAIL_WRITES; i++ {

				if conn := bw.getConn(); conn != nil {

					log.Debugf("Bulk inserting %d recs into %q (host: %s)", len(toWrite), bw.table, conn.GetHost())

					if err := query.Exec(conn); err == nil {

						log.Debugf("Db %q: %d", bw.table, len(toWrite))
						toWrite = toWrite[:0]
						break

					} else {
						log.Warningf("Bulk failed %d recs into %q (host: %s): %s", len(toWrite), bw.table, conn.GetHost(), err)
					}
				} else {
					log.Warningf("No active connections to db")
				}

				time.Sleep(time.Second)

			}

			if len(toWrite) > 0 {
				log.Errorf("Too many fails, lost %d recs! Dump:%++v", len(toWrite), toWrite)
				toWrite = toWrite[:0]
			}

		}

		if needExit {
			return
		}

	}

}
