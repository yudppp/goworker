package goworker

import (
	"testing"
	"time"
)

var intervalDurationParseTests = []struct {
	v        string
	expected intervalDuration
}{
	{"0", intervalDuration(0)},
	{"1", intervalDuration(1 * time.Second)},
	{"1.5", intervalDuration(1500 * time.Millisecond)},
}

func TestParseInterval(t *testing.T) {
	for _, tt := range intervalDurationParseTests {
		actual := new(intervalDuration)
		if err := actual.parse(tt.v); err != nil {
			t.Errorf("IntervalFlag(%#v): set to %s error %s", actual, tt.v, err)
		} else {
			if *actual != tt.expected {
				t.Errorf("IntervalFlag: set to %s expected %v, actual %v", tt.v, tt.expected, actual)
			}
		}
	}
}

var intervalDurationSetFloatTests = []struct {
	v        float64
	expected intervalDuration
}{
	{0.0, intervalDuration(0)},
	{1.0, intervalDuration(1 * time.Second)},
	{1.5, intervalDuration(1500 * time.Millisecond)},
}

func TestIntervalFlagSetFloat(t *testing.T) {
	for _, tt := range intervalDurationSetFloatTests {
		actual := new(intervalDuration)
		if err := actual.setFloat(tt.v); err != nil {
			t.Errorf("IntervalFlag(%#v): set to %f error %s", actual, tt.v, err)
		} else {
			if *actual != tt.expected {
				t.Errorf("IntervalFlag: set to %f expected %v, actual %v", tt.v, tt.expected, actual)
			}
		}
	}
}

var intervalDurationStringTests = []struct {
	d        intervalDuration
	expected string
}{
	{
		intervalDuration(0),
		"0",
	},
	{
		intervalDuration(1 * time.Second),
		"1000000000",
	},
	{
		intervalDuration(1500 * time.Millisecond),
		"1500000000",
	},
}

func TestIntervalDurationString(t *testing.T) {
	for _, tt := range intervalDurationStringTests {
		actual := tt.d.String()
		if actual != tt.expected {
			t.Errorf("IntervalFlag(%#v): expected %s, actual %s", tt.d, tt.expected, actual)
		}
	}
}
