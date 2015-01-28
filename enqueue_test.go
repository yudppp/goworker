package goworker

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"testing"
	"time"
)

func TestEnqueueHasNoDeadlock(t *testing.T) {
	p := newRedisPool(uri, 1, 1, time.Minute)
	defer p.Close()

	exitOnComplete = true
	queues.Set("test_enqueue_has_no_deadlock")
	jobProcessed := false
	Register("NoDeadLock", func(q string, args ...interface{}) error {
		Enqueue("dummy", "Dummy", nil)
		jobProcessed = true
		return nil
	})
	Enqueue("test_enqueue_has_no_deadlock", "NoDeadLock", nil)
	err := WorkWithPool(p)
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
	defer conn.Do("DEL", fmt.Sprintf("%squeue:dummy", namespace))
	defer conn.Do("DEL", fmt.Sprintf("%squeue:test_enqueue_has_no_deadlock", namespace))
}

func TestEnqueueWriteToRedis(t *testing.T) {
	p := newRedisPool(uri, 1, 1, time.Minute)
	defer p.Close()

	queues.Set("no")
	Enqueue("test2", "TestEnqueueWriteToRedis", nil)
	WorkWithPool(p)
	resource, _ := pool.Get()
	conn := resource.(*redisConn)
	defer pool.Put(conn)
	defer conn.Do("DEL", fmt.Sprintf("%squeue:test2", namespace))
	res, err := conn.Do("LPOP", fmt.Sprintf("%squeue:test2", namespace))
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
	p := newRedisPool(uri, 1, 1, time.Minute)
	defer p.Close()

	queues.Set("no")

	args := make([]interface{}, 5)
	args[0] = "foo"
	args[1] = "bar"
	args[2] = true
	args[3] = 1
	args[4] = 0.999999

	EnqueueUnique("test3", "TestEnqueueUniqueWriteToRedis", args)
	EnqueueUnique("test3", "TestEnqueueUniqueWriteToRedis", args)

	WorkWithPool(p)
	resource, _ := pool.Get()
	conn := resource.(*redisConn)
	defer pool.Put(conn)
	defer conn.Do("DEL", fmt.Sprintf("%squeue:test3", namespace))
	res, err := redis.Int(conn.Do("LLEN", fmt.Sprintf("%squeue:test3", namespace)))
	if err != nil {
		t.Errorf("%v", err)
	}
	if res != 1 {
		t.Errorf("expecting 1 job in queue, but got %d jobs", res)
	}
}
