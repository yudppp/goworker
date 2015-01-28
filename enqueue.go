package goworker

import (
	"encoding/json"
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// EnqueueUnique function let you asynchronously enqueue a new job in Resque given
// the queue, the class name and the parameters if the job does NOT already exist.
//
// param queue: name of the queue (not including the namespace)
// param class: name of the Worker that can handle this job
// param args:  arguments to pass to the handler function. Must be the non-marshalled version.
//
// return an error if args cannot be marshalled
func EnqueueUnique(queue string, class string, args []interface{}) (err error) {

	var conn *redisConn

	// Retrieve a connection from the pool or create a new one if no pool is opened.
	if pool != nil && !pool.IsClosed() {
		resource, err := pool.Get()
		if err != nil {
			return err
		}
		conn = resource.(*redisConn)
		defer pool.Put(conn)
	} else {
		// non-optimized mode, create a pool to avoid getting there.
		conn, err = redisConnFromUri(uri)
		if err != nil {
			return err
		}
		defer conn.Close()
	}

	jobIsUnique := true

	// Check if job already exists in Redis
	messages, _ := redis.Strings(conn.Do("lrange", fmt.Sprintf("%squeue:%s", namespace, queue), 0, -1))

	for _, msg := range messages {

		var result map[string]interface{}
		json.Unmarshal([]byte(msg), &result)

		resultClass := result["class"]
		resultArgs := fmt.Sprintf("%v", result["args"].(interface{}))
		inArgs := fmt.Sprintf("%v", args)

		if resultClass == class && resultArgs == inArgs {
			jobIsUnique = false
			break
		}
	}

	conn.Flush()

	if jobIsUnique {
		return Enqueue(queue, class, args)
	} else {
		logger.Infof("not enqueueing duplicate msg in queue %s | class: %s | args: %v", queue, class, args)
		return nil
	}
}

// Enqueue function let you asynchronously enqueue a new job in Resque given
// the queue, the class name and the parameters.
//
// param queue: name of the queue (not including the namespace)
// param class: name of the Worker that can handle this job
// param args:  arguments to pass to the handler function. Must be the non-marshalled version.
//
// return an error if args cannot be marshalled
func Enqueue(queue string, class string, args []interface{}) (err error) {
	data := &payload{
		Class: class,
		Args:  args,
	}
	b, err := json.Marshal(data)
	if err != nil {
		return
	}

	var conn *redisConn

	// Retrieve a connection from the pool or create a new one if no pool is opened.
	if pool != nil && !pool.IsClosed() {
		resource, err := pool.Get()
		if err != nil {
			return err
		}
		conn = resource.(*redisConn)
		defer pool.Put(conn)
	} else {
		// non-optimized mode, create a pool to avoid getting there.
		conn, err = redisConnFromUri(uri)
		if err != nil {
			return err
		}
		defer conn.Close()
	}

	// Push job in redis
	err = conn.Send("RPUSH", fmt.Sprintf("%squeue:%s", namespace, queue), b)
	if err != nil {
		return
	}

	conn.Flush()
	return
}
