// Running goworker
//
// After building your workers, you will have an
// executable that you can run which will
// automatically poll a Redis server and call
// your workers as jobs arrive.
//
// Configuration
//
// There are several parameters which control the
// operation of the goworker client.
//
// -queues="comma,delimited,queues"
// — This is the only required parameter. The
// recommended practice is to separate your
// Resque workers from your goworkers with
// different queues. Otherwise, Resque worker
// classes that have no goworker analog will
// cause the goworker process to fail the jobs.
// Because of this, there is no default queue,
// nor is there a way to select all queues (à la
// Resque's * queue). Queues are processed in
// the order they are specififed.
// If you have multiple queues you can assign
// them weights. A queue with a weight of 2 will
// be checked twice as often as a queue with a
// weight of 1: -queues='high=2,low=1'.
//
// -interval=5.0
// — Specifies the wait period between polling if
// no job was in the queue the last time one was
// requested.
//
// -concurrency=10
// — Specifies the number of concurrently
// executing workers. This number can be as low
// as 1 or rather comfortably as high as 100,000,
// and should be tuned to your workflow and the
// availability of outside resources.
//
// -connections=2
// — Specifies the maximum number of Redis
// connections that goworker will consume between
// the poller and all workers. There is not much
// performance gain over two and a slight penalty
// when using only one. This is configurable in
// case you need to keep connection counts low
// for cloud Redis providers who limit plans on
// maxclients.
//
// -uri=redis://localhost:6379/
// — Specifies the URI of the Redis database from
// which goworker polls for jobs. Accepts URIs of
// the format redis://user:pass@host:port/db or
// unix:///path/to/redis.sock.
//
// -namespace=resque:
// — Specifies the namespace from which goworker
// retrieves jobs and stores stats on workers.
//
// -exit-on-complete=false
// — Exits goworker when there are no jobs left
// in the queue. This is helpful in conjunction
// with the time command to benchmark different
// configurations.
//
package goworker

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type intervalOption time.Duration
type queuesOption []string

type config struct {
	queues         queuesOption
	interval       intervalOption
	concurrency    int
	connections    int
	uri            string
	namespace      string
	exitOnComplete bool
	isStrict       bool
}

var (
	errorEmptyQueues      = errors.New("You must specify at least one queue.")
	errorNonNumericWeight = errors.New("The weight must be a numeric value.")
)

var cfg *config

func init() {

	cfg = &config{}

	Configure(map[string]string{
		"queues":         "",
		"interval":       "5.0",
		"concurrency":    "10",
		"connections":    "2",
		"uri":            "redis://localhost:6379/",
		"namespace":      "resque:",
		"exitOnComplete": "false",
		"isStrict":       "true"})
}

func Configure(options map[string]string) {

	var err error

	if value, ok := options["queues"]; ok {
		cfg.queues = strings.Split(value, ",")
		cfg.isStrict = strings.IndexRune(value, '=') == -1
	}

	if value, ok := options["interval"]; ok {
		var i intervalOption
		if err = i.parse(value); err != nil {
			panic(err)
		} else {
			cfg.interval = i
		}
	}

	if value, ok := options["concurrency"]; ok {
		if cfg.concurrency, err = strconv.Atoi(value); err != nil {
			panic(err)
		}
	}

	if value, ok := options["connections"]; ok {
		if cfg.connections, err = strconv.Atoi(value); err != nil {
			panic(err)
		}
	}

	if value, ok := options["uri"]; ok {
		cfg.uri = value
	}

	if value, ok := options["namespace"]; ok {
		cfg.namespace = value
	}

	if value, ok := options["exitOnComplete"]; ok {
		if cfg.exitOnComplete, err = strconv.ParseBool(value); err != nil {
			panic(err)
		}
	}
}

func PrintConfig() string {

	return fmt.Sprintf("queues: %v | interval: %v", cfg.queues, cfg.interval.secondsString()) +
		fmt.Sprintf(" | concurrency: %v | connections: %v", cfg.concurrency, cfg.connections) +
		fmt.Sprintf(" | uri: %v | namespace: %v", cfg.uri, cfg.namespace) +
		fmt.Sprintf(" | exitOnComplete: %v", cfg.exitOnComplete)
}

func (d *intervalOption) parse(value string) error {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return err
	}
	d.setFloat(f)
	return nil
}

func (d *intervalOption) setFloat(value float64) error {
	*d = intervalOption(time.Duration(value * float64(time.Second)))
	return nil
}

func (d *intervalOption) String() string {
	return fmt.Sprint(*d)
}

func (d *intervalOption) secondsString() string {
	s := d.String()
	f, _ := strconv.ParseFloat(s, 64)
	return fmt.Sprint(f / 1E9)
}

func (q *queuesOption) Set(value string) error {
	for _, queueAndWeight := range strings.Split(value, ",") {
		if queueAndWeight == "" {
			continue
		}

		queue, weight, err := parseQueueAndWeight(queueAndWeight)
		if err != nil {
			return err
		}

		for i := 0; i < weight; i++ {
			*q = append(*q, queue)
		}
	}
	if len(*q) == 0 {
		return errorEmptyQueues
	}
	return nil
}

func (q *queuesOption) String() string {
	return fmt.Sprint(*q)
}

func parseQueueAndWeight(queueAndWeight string) (queue string, weight int, err error) {
	parts := strings.SplitN(queueAndWeight, "=", 2)
	queue = parts[0]

	if queue == "" {
		return
	}

	if len(parts) == 1 {
		weight = 1
	} else {
		weight, err = strconv.Atoi(parts[1])
		if err != nil {
			err = errorNonNumericWeight
		}
	}
	return
}
