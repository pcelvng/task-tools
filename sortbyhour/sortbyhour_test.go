package sortbyhour

import (
	"testing"
	"time"
)

func TestWorker_parseJson(t *testing.T) {
	var cases = []struct {
		msg       string
		worker    Worker
		json      string
		expected  time.Time
		shouldErr bool
	}{
		{
			"Correct raw message",
			Worker{
				TimeFormat:  time.RFC3339,
				JsonTimeTag: "dt",
			},
			`{"dt":"2017-11-01T12:34:56Z"}`,
			mustParse("2017-11-01T12:34:56Z"),
			false,
		},
		{
			"Missing raw time",
			Worker{
				TimeFormat:  time.RFC3339,
				JsonTimeTag: "dt",
			},
			`{"timestamp":"2017-11-01T12:34:56Z"}`,
			mustParse("2017-11-01T12:34:56Z"),
			true,
		},
		{
			"Invalid time",
			Worker{
				TimeFormat:  time.RFC3339,
				JsonTimeTag: "timestamp",
			},
			`{"timestamp":"2017-11-01T12:34:56"}`,
			mustParse("2017-11-01T12:34:56Z"),
			true,
		},
		{
			"Bad raw data",
			Worker{
				TimeFormat:  time.RFC3339,
				JsonTimeTag: "dt",
			},
			`"timestamp":"2017-11-01T12:34:56Z"`,
			mustParse("2017-11-01T12:34:56Z"),
			true,
		},
	}
	for _, test := range cases {
		tm, err := (&test).worker.processJson([]byte(test.json))
		if test.shouldErr != (err != nil) {
			t.Errorf("FAIL: %s Err mismatch %s", test.msg, err)
		} else if !test.shouldErr && !tm.Equal(test.expected) {
			t.Errorf("FAIL: %s time does not match", test.msg)
		} else {
			t.Logf("PASS: %s", test.msg)
		}
	}
}

func TestWorker_ParseTab(t *testing.T) {
	var cases = []struct {
		msg       string
		worker    Worker
		raw       string
		expected  time.Time
		shouldErr bool
	}{
		{
			"time at index 0",
			Worker{
				TimeFormat: "Jan/02/2006 15:04:05",
				TimeIndex:  0,
			},
			"Jan/10/2017 12:15:27",
			mustParse("2017-01-10T12:15:27Z"),
			false,
		},
		{
			"time at last index",
			Worker{
				TimeFormat: "Jan/02/2006 15:04:05",
				TimeIndex:  2,
			},
			"asdlfj;asjdkf|more data|Jan/10/2017 12:15:27",
			mustParse("2017-01-10T12:15:27Z"),
			false,
		},
		{
			"out of bound index",
			Worker{
				TimeFormat: "Jan/02/2006 15:04:05",
				TimeIndex:  10,
			},
			"asdlfj;asjdkf|more data|Jan/10/2017 12:15:27",
			mustParse("2017-01-10T12:15:27Z"),
			true,
		},
	}
	for _, test := range cases {
		tm, err := (&test).worker.processTab([]byte(test.raw))
		if test.shouldErr != (err != nil) {
			t.Errorf("FAIL: '%s' Err mismatch - %s", test.msg, err)
		} else if !test.shouldErr && !tm.Equal(test.expected) {
			t.Errorf("FAIL: %s time does not match", test.msg)
		} else {
			t.Logf("PASS: %s", test.msg)
		}
	}
}

func mustParse(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
