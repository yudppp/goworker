package goworker

import (
	"encoding/json"
	"fmt"
	"github.com/yudppp/goworker/_vendar/vitess/go/pools"

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
func enqueue(p *pools.ResourcePool, queue string, class string, args []interface{}, dedupe bool) (err error) {

	if dedupe {

		if isJobUnique(p, queue, class, args) {
			err = addToQueue(p, queue, class, args)
		} else {
			logger.Infof("not enqueueing duplicate msg in queue %s | class: %s | args: %v", queue, class, args)
		}
	} else {
		err = addToQueue(p, queue, class, args)
	}

	return
}

func addToQueue(p *pools.ResourcePool, queue string, class string, args []interface{}) (err error) {

	var conn *redisConn

	resource, err := p.Get()
	if err != nil {
		logger.Criticalf("Error on getting connection to enqueue job: %v", err)
	} else {
		conn = resource.(*redisConn)
		defer p.Put(conn)
	}

	data := &payload{
		Class: class,
		Args:  args,
	}

	b, err := json.Marshal(data)
	if err != nil {
		return
	}

	// Push job in redis
	_, err = conn.Do("RPUSH", fmt.Sprintf("%squeue:%s", cfg.namespace, queue), b)
	if err != nil {
		return
	}

	return

}

func isJobUnique(p *pools.ResourcePool, queue string, class string, args []interface{}) bool {

	var conn *redisConn

	isUnique := true

	resource, err := p.Get()
	if err != nil {
		logger.Criticalf("Error on getting connection to check if job is already in queue: %v", err)
	} else {
		conn = resource.(*redisConn)
		defer p.Put(conn)
	}

	// Check if job already exists in Redis
	messages, _ := redis.Strings(conn.Do("lrange", fmt.Sprintf("%squeue:%s", cfg.namespace, queue), 0, -1))

	for _, msg := range messages {

		var result map[string]interface{}
		json.Unmarshal([]byte(msg), &result)

		resultClass := result["class"]
		resultArgs := fmt.Sprintf("%v", result["args"].(interface{}))
		inArgs := fmt.Sprintf("%v", args)

		if resultClass == class && resultArgs == inArgs {
			isUnique = false
			break
		}
	}

	return isUnique
}
