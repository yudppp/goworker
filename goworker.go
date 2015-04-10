package goworker

import (
	"github.com/cihub/seelog"
	"github.com/yudppp/goworker/_vendar/vitess/go/pools"
	"os"
	"strconv"
	"sync"
	"time"
)

var logger seelog.LoggerInterface

func init() {
	if err := initLogger(); err != nil {
		panic(err)
	}
}

// Call this function to run goworker. Check for errors in
// the return value. Work will take over the Go executable
// and will run until a QUIT, INT, or TERM signal is
// received, or until the queues are empty if the
// -exit-on-complete flag is set.
func Work() error {
	p := newRedisPool(cfg.uri, cfg.connections, cfg.connections, time.Minute)
	defer p.Close()
	return startWorkerWithPool(p)
}

/*
Enqueue puts a job in the queue.  If you don't want duplicate jobs,
then set dedupe = true
*/
func Enqueue(queue string, class string, args []interface{}, dedupe bool) error {
	p := newRedisPool(cfg.uri, cfg.connections, cfg.connections, time.Minute)
	defer p.Close()
	return startEnqueuerWithPool(p, queue, class, args, dedupe)
}

func EnqueueWithPool(p *pools.ResourcePool, queue string, class string, args []interface{}, dedupe bool) error {
	return startEnqueuerWithPool(p, queue, class, args, dedupe)
}

func startEnqueuerWithPool(p *pools.ResourcePool, queue string, class string, args []interface{}, dedupe bool) error {
	pool = p
	return enqueue(p, queue, class, args, dedupe)
}

// Call this function to run goworker with the given pool.
func WorkWithPool(pool *pools.ResourcePool) error {
	return startWorkerWithPool(pool)
}

func initLogger() error {
	var err error
	logger, err = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.InfoLvl)
	if err != nil {
		return err
	}
	return nil
}

// Start worker with the given pool.
func startWorkerWithPool(p *pools.ResourcePool) error {
	pool = p
	quit := signals()

	poller, err := newPoller(cfg.queues, cfg.isStrict)
	if err != nil {
		return err
	}
	jobs := poller.poll(p, time.Duration(cfg.interval), quit)

	var monitor sync.WaitGroup

	for id := 0; id < cfg.concurrency; id++ {
		worker, err := newWorker(strconv.Itoa(id), cfg.queues)
		if err != nil {
			return err
		}
		worker.work(p, jobs, &monitor)
	}

	monitor.Wait()
	return nil
}
