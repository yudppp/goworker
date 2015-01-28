package goworker

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"testing"
	"time"
)

func TestEnqueueHasNoDeadlock(t *testing.T) {

	var err error
	p := newRedisPool(cfg.uri, 1, 1, time.Minute)
	defer p.Close()

	cfg.exitOnComplete = true
	cfg.queues = []string{"test_enqueue_has_no_deadlock"}
	jobProcessed := false
	Register("NoDeadLock", func(q string, args ...interface{}) error {
		EnqueueWithPool(p, "dummy", "Dummy", nil, false)
		jobProcessed = true
		return nil
	})

	err = EnqueueWithPool(p, "test_enqueue_has_no_deadlock", "NoDeadLock", nil, false)
	err = WorkWithPool(p)
	if !jobProcessed {
		t.Error("job has not been processed")
	}
	if err != nil {
		t.Errorf("Error occured %v", err)
	}
	if p.IsClosed() {
		t.Error("Pool should not be closed")
	}

	resource, _ := p.Get()
	conn := resource.(*redisConn)
	defer p.Put(conn)
	defer conn.Do("DEL", fmt.Sprintf("%squeue:dummy", cfg.namespace))
	defer conn.Do("DEL", fmt.Sprintf("%squeue:test_enqueue_has_no_deadlock", cfg.namespace))
}

func TestEnqueueWriteToRedis(t *testing.T) {

	p := newRedisPool(cfg.uri, 1, 1, time.Minute)
	defer p.Close()

	cfg.queues = []string{"no"}
	EnqueueWithPool(p, "test2", "TestEnqueueWriteToRedis", nil, false)
	resource, _ := p.Get()
	conn := resource.(*redisConn)
	defer p.Put(conn)
	defer conn.Do("DEL", fmt.Sprintf("%squeue:test2", cfg.namespace))
	res, err := conn.Do("LPOP", fmt.Sprintf("%squeue:test2", cfg.namespace))
	if err != nil {
		t.Errorf("%v", err)
	}
	jsonData, _ := redis.Bytes(res, nil)
	var data map[string]interface{}
	json.Unmarshal(jsonData, &data)
	if data["class"] != "TestEnqueueWriteToRedis" {
		t.Error(data["class"])
	}
}

func TestEnqueueUniqueWriteToRedis(t *testing.T) {
	p := newRedisPool(cfg.uri, 1, 1, time.Minute)
	defer p.Close()

	cfg.queues = []string{"no"}

	args := make([]interface{}, 5)
	args[0] = "foo"
	args[1] = "bar"
	args[2] = true
	args[3] = 1
	args[4] = 0.999999

	EnqueueWithPool(p, "test3", "TestEnqueueUniqueWriteToRedis", args, true)
	EnqueueWithPool(p, "test3", "TestEnqueueUniqueWriteToRedis", args, true)

	resource, _ := p.Get()
	conn := resource.(*redisConn)
	defer p.Put(conn)
	defer conn.Do("DEL", fmt.Sprintf("%squeue:test3", cfg.namespace))
	res, err := redis.Int(conn.Do("LLEN", fmt.Sprintf("%squeue:test3", cfg.namespace)))
	if err != nil {
		t.Errorf("%v", err)
	}
	if res != 1 {
		t.Errorf("expecting 1 job in queue, but got %d jobs", res)
	}
}
