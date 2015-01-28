package goworker

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

var intervalOptionParseTests = []struct {
	v        string
	expected intervalOption
}{
	{"0", intervalOption(0)},
	{"1", intervalOption(1 * time.Second)},
	{"1.5", intervalOption(1500 * time.Millisecond)},
}

func TestParseInterval(t *testing.T) {
	for _, tt := range intervalOptionParseTests {
		actual := new(intervalOption)
		if err := actual.parse(tt.v); err != nil {
			t.Errorf("IntervalDuration(%#v): set to %s error %s", actual, tt.v, err)
		} else {
			if *actual != tt.expected {
				t.Errorf("IntervalDuration: set to %s expected %v, actual %v", tt.v, tt.expected, actual)
			}
		}
	}
}

var intervalOptionSetFloatTests = []struct {
	v        float64
	expected intervalOption
}{
	{0.0, intervalOption(0)},
	{1.0, intervalOption(1 * time.Second)},
	{1.5, intervalOption(1500 * time.Millisecond)},
}

func TestIntervalFlagSetFloat(t *testing.T) {
	for _, tt := range intervalOptionSetFloatTests {
		actual := new(intervalOption)
		if err := actual.setFloat(tt.v); err != nil {
			t.Errorf("IntervalDuration(%#v): set to %f error %s", actual, tt.v, err)
		} else {
			if *actual != tt.expected {
				t.Errorf("IntervalDuration: set to %f expected %v, actual %v", tt.v, tt.expected, actual)
			}
		}
	}
}

var intervalOptionStringTests = []struct {
	d        intervalOption
	expected string
}{
	{
		intervalOption(0),
		"0",
	},
	{
		intervalOption(1 * time.Second),
		"1000000000",
	},
	{
		intervalOption(1500 * time.Millisecond),
		"1500000000",
	},
}

func TestIntervalDurationString(t *testing.T) {
	for _, tt := range intervalOptionStringTests {
		actual := tt.d.String()
		if actual != tt.expected {
			t.Errorf("IntervalDuration(%#v): expected %s, actual %s", tt.d, tt.expected, actual)
		}
	}
}

var queuesOptionSetTests = []struct {
	v        string
	expected queuesOption
	err      error
}{
	{
		"",
		nil,
		errors.New("You must specify at least one queue."),
	},
	{
		"high",
		queuesOption([]string{"high"}),
		nil,
	},
	{
		"high,low",
		queuesOption([]string{"high", "low"}),
		nil,
	},
	{
		"high=2,low=1",
		queuesOption([]string{"high", "high", "low"}),
		nil,
	},
	{
		"high=2,low",
		queuesOption([]string{"high", "high", "low"}),
		nil,
	},
	{
		"low=1,high=2",
		queuesOption([]string{"low", "high", "high"}),
		nil,
	},
	{
		"low=,high=2",
		nil,
		errors.New("The weight must be a numeric value."),
	},
	{
		"low=a,high=2",
		nil,
		errors.New("The weight must be a numeric value."),
	},
	{
		"low=",
		nil,
		errors.New("The weight must be a numeric value."),
	},
	{
		"low=a",
		nil,
		errors.New("The weight must be a numeric value."),
	},
	{
		"high=2,,,=1",
		queuesOption([]string{"high", "high"}),
		nil,
	},
	{
		",,,",
		nil,
		errors.New("You must specify at least one queue."),
	},
	{
		"=1",
		nil,
		errors.New("You must specify at least one queue."),
	},
}

func TestQueuesOptionSet(t *testing.T) {
	for _, tt := range queuesOptionSetTests {
		actual := new(queuesOption)
		err := actual.Set(tt.v)
		if fmt.Sprint(actual) != fmt.Sprint(tt.expected) {
			t.Errorf("QueuesOption: set to %s expected %v, actual %v", tt.v, tt.expected, actual)
		}
		if (err != nil && tt.err == nil) ||
			(err == nil && tt.err != nil) ||
			(err != nil && tt.err != nil && err.Error() != tt.err.Error()) {
			t.Errorf("QueuesOption: set to %s expected err %v, actual err %v", tt.v, tt.err, err)
		}
	}
}

var queuesOptionStringTests = []struct {
	q        queuesOption
	expected string
}{
	{
		queuesOption([]string{"high"}),
		"[high]",
	},
	{
		queuesOption([]string{"high", "low"}),
		"[high low]",
	},
}

func TestQueuesOptionString(t *testing.T) {
	for _, tt := range queuesOptionStringTests {
		actual := tt.q.String()
		if actual != tt.expected {
			t.Errorf("QueuesOption(%#v): expected %s, actual %s", tt.q, tt.expected, actual)
		}
	}
}
