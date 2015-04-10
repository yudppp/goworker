package goworker

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/youtube/vitess/go/pools"
	"sync"
	"time"
)

type worker struct {
	process
}

func newWorker(id string, queues []string) (*worker, error) {
	process, err := newProcess(id, queues)
	if err != nil {
		return nil, err
	}
	return &worker{
		process: *process,
	}, nil
}

func (w *worker) MarshalJSON() ([]byte, error) {
	return json.Marshal(w.String())
}

func (w *worker) start(conn *redisConn, job *job) error {
	work := &work{
		Queue:   job.Queue,
		RunAt:   time.Now(),
		Payload: job.Payload,
	}

	buffer, err := json.Marshal(work)
	if err != nil {
		return err
	}

	conn.Send("SET", fmt.Sprintf("%sworker:%s", cfg.namespace, w), buffer)
	logger.Debugf("Processing %s since %s [%v]", work.Queue, work.RunAt, work.Payload.Class)

	return w.process.start(conn)
}

func (w *worker) fail(conn *redisConn, job *job, err error) error {
	failure := &failure{
		FailedAt:  time.Now(),
		Payload:   job.Payload,
		Exception: "Error",
		Error:     err.Error(),
		Worker:    w,
		Queue:     job.Queue,
	}
	buffer, err := json.Marshal(failure)
	if err != nil {
		return err
	}
	conn.Send("RPUSH", fmt.Sprintf("%sfailed", cfg.namespace), buffer)

	return w.process.fail(conn)
}

func (w *worker) succeed(conn *redisConn, job *job) error {
	conn.Send("INCR", fmt.Sprintf("%sstat:processed", cfg.namespace))
	conn.Send("INCR", fmt.Sprintf("%sstat:processed:%s", cfg.namespace, w))

	return nil
}

func (w *worker) finish(conn *redisConn, job *job, err error) error {
	if err != nil {
		w.fail(conn, job, err)
	} else {
		w.succeed(conn, job)
	}
	return w.process.finish(conn)
}

func (w *worker) work(pool *pools.ResourcePool, jobs <-chan *job, monitor *sync.WaitGroup) {
	resource, err := pool.Get()
	if err != nil {
		logger.Criticalf("Error on getting connection in worker %v", w)
	} else {
		conn := resource.(*redisConn)
		w.open(conn)
		pool.Put(conn)
	}

	monitor.Add(1)

	go func() {
		defer func() {
			resource, err := pool.Get()
			if err != nil {
				logger.Criticalf("Error on getting connection in worker %v", w)
			} else {
				conn := resource.(*redisConn)
				w.close(conn)
				pool.Put(conn)
			}

			monitor.Done()
		}()
		for job := range jobs {
			if workerFunc, ok := workers[job.Payload.Class]; ok {
				w.run(pool, job, workerFunc)

				logger.Debugf("done: (Job{%s} | %s | %v)", job.Queue, job.Payload.Class, job.Payload.Args)
			} else {
				errorLog := fmt.Sprintf("No worker for %s in queue %s with args %v", job.Payload.Class, job.Queue, job.Payload.Args)
				logger.Critical(errorLog)

				resource, err := pool.Get()
				if err != nil {
					logger.Criticalf("Error on getting connection in worker %v", w)
				} else {
					conn := resource.(*redisConn)
					w.finish(conn, job, errors.New(errorLog))
					pool.Put(conn)
				}
			}
		}
	}()
}

func (w *worker) run(pool *pools.ResourcePool, job *job, workerFunc workerFunc) {
	var err error
	defer func() {
		resource, poolErr := pool.Get()
		if poolErr != nil {
			logger.Criticalf("Error on getting connection in worker %v", w)
		} else {
			conn := resource.(*redisConn)
			w.finish(conn, job, err)
			pool.Put(conn)
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprint(r))
		}
	}()

	resource, err := pool.Get()
	if err != nil {
		logger.Criticalf("Error on getting connection in worker %v", w)
	} else {
		conn := resource.(*redisConn)
		w.start(conn, job)
		pool.Put(conn)
	}
	err = workerFunc(job.Queue, job.Payload.Args...)
}
