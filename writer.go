package yadb

import (
	"github.com/roistat/go-clickhouse"
	log "github.com/sirupsen/logrus"

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
	workers = make(chan chan clickhouse.Row, MAX_WORKERS)
	wg      sync.WaitGroup
)

func (bw *BatchWriter) getObjects(obj_chan chan clickhouse.Row, tick time.Duration, done *sync.WaitGroup) {

	to_write := clickhouse.Rows{}
	tickChan := time.NewTicker(tick).C

	need_exit := false
	defer done.Done()

	for {

		need_write := false

		select {

		case item, ok := <-obj_chan:

			if !ok {
				log.Debugf("CL goroutine %q exiting...", bw.table)
				need_exit = true
				for x := range obj_chan {
					to_write = append(to_write, x)
				}
			} else {
				to_write = append(to_write, item)
				// log.Debug("New elem")
			}

			if len(to_write) > bw.bulk_items {
				need_write = true
			}

		case <-tickChan:
			need_write = true

		}

		if (need_exit || need_write) && len(to_write) > 0 {

			for i := 0; i < FAIL_WRITES; i++ {

				// log.Debugf("Start writing (%d) objects to (%s)...", len(to_write), bw.table)

				if bw.getConn == nil {
					log.Debug("DB skip...")
					to_write = to_write[:0]
					break
				}

				query, err := clickhouse.BuildMultiInsert(bw.table,
					bw.columns,
					to_write,
				)

				if err == nil {

					conn := bw.getConn()

					err = query.Exec(conn)

					if err == nil {

						log.Debugf("Db %q: %d", bw.table, len(to_write))
						to_write = to_write[:0]
						break

					} else {
						log.Warningf("Error db %q: %s", bw.table, err)
					}

				} else {
					log.Errorf("Build %q request fail: %s - %v", bw.table, err, to_write)
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

func Wait() {

	log.Debug("Clickhouse goroutines exiting...")
	// закрываем буферезированный канал воркеров чтоб выйти из него range-ом
	close(workers)

	// получаем все ранее созданные каналы
	for worker := range workers {

		// закрываем каждый из этих каналов чтоб они завершились
		close(worker)
	}

	// ждем пока все горутины допишут и выйдут
	wg.Wait()
}
