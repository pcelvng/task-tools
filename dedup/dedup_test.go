package dedup

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pcelvng/task"
)

func TestNewWorker(t *testing.T) {
	cases := []struct {
		msg       string
		info      string
		shouldErr bool
	}{
		{
			msg:  "Valid Worker",
			info: "?Key=key&TimeField=time&ReadPath=/&WritePath=nop://",
		},
		{
			msg:       "Invalid Worker - Bad URI",
			info:      "://",
			shouldErr: true,
		},
		{
			msg:       "Invalid Worker - bad reader",
			info:      "invalid://host?WritePath=nop",
			shouldErr: true,
		},
		{
			msg:       "Invalid Worker - bad writer",
			info:      "?Key=key&TimeField=time",
			shouldErr: true,
		},
	}
	for _, test := range cases {
		w := (&Config{}).NewWorker(test.info)
		if invalid, msg := task.IsInvalidWorker(w); invalid != test.shouldErr {
			t.Errorf("FAIL: %s %s", test.msg, msg)
		} else {
			t.Logf("PASS: %s %s", test.msg, msg)
		}

	}
}

func ExampleWorker_DoTask() {
	w := (&Config{}).NewWorker("?Key=field1,field2&TimeFile=timestamp&WritePath=nop://")
	r, s := w.DoTask(nil)
	fmt.Println(r, s)
	// Output: complete Lines written: 0
}

func TestWorker_Dedup(t *testing.T) {
	cases := []struct {
		msg       string
		worker    *Worker
		data      []string
		result    map[string]string
		shouldErr bool
	}{
		{
			msg:    "1 entry",
			worker: defaultWorker(),
			data:   []string{`{"key":"a","time":"2018-01-01T01:01:01Z"}`},
			result: map[string]string{"a": `{"key":"a","time":"2018-01-01T01:01:01Z"}`},
		},
		{
			msg:    "dedup - keep newest",
			worker: defaultWorker(),
			data: []string{
				`{"key":"a","time":"2018-01-01T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-01T01:02:01Z"}`,
				`{"key":"a","time":"2018-02-01T01:02:01Z"}`,
				`{"key":"a","time":"2018-02-02T01:02:01Z"}`,
			},
			result: map[string]string{"a": `{"key":"a","time":"2018-02-02T01:02:01Z"}`},
		},
		{
			msg: "dedup - keep oldest",
			worker: &Worker{
				Key:       []string{"key"},
				TimeField: "time",
				Keep:      Oldest,
				data:      make(map[string]string),
			},
			data: []string{
				`{"key":"a","time":"2018-01-03T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-02T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-01T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-04T01:01:01Z"}`,
			},
			result: map[string]string{"a": `{"key":"a","time":"2018-01-01T01:01:01Z"}`},
		},
		{
			msg: "dedup - keep first",
			worker: &Worker{
				Key:       []string{"key"},
				TimeField: "time",
				Keep:      First,
				data:      make(map[string]string),
			},
			data: []string{
				`{"key":"a","time":"2018-01-03T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-02T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-01T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-04T01:01:01Z"}`,
			},
			result: map[string]string{"a": `{"key":"a","time":"2018-01-03T01:01:01Z"}`},
		},
		{
			msg: "dedup - keep last",
			worker: &Worker{
				Key:       []string{"key"},
				TimeField: "time",
				Keep:      Last,
				data:      make(map[string]string),
			},
			data: []string{
				`{"key":"a","time":"2018-01-03T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-02T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-01T01:01:01Z"}`,
				`{"key":"a","time":"2018-01-04T01:01:01Z"}`,
			},
			result: map[string]string{"a": `{"key":"a","time":"2018-01-04T01:01:01Z"}`},
		},
		{
			msg: "multiple keys",
			worker: &Worker{
				Key:       []string{"key1", "key2", "key3"},
				TimeField: "time",
				data:      make(map[string]string),
			},
			data:   []string{`{"key1":"a","key2":"b","key3":"c","time":"2018-01-01T01:01:01Z"}`},
			result: map[string]string{"a|b|c": `{"key1":"a","key2":"b","key3":"c","time":"2018-01-01T01:01:01Z"}`},
		},
		{
			msg: "dedup - multiple keys (newest)",
			worker: &Worker{
				Key:       []string{"key1", "key2", "key3"},
				TimeField: "time",
				data:      make(map[string]string),
			},
			data: []string{
				`{"key1":"a","key2":"b","key3":"c","time":"2018-02-01T01:01:01Z"}`,
				`{"key3":"c","key1":"a","key2":"b","time":"2018-01-01T01:01:01Z"}`,
				`{"key2":"b","key1":"a","key3":"c","time":"2018-01-02T01:01:01Z"}`,
			},
			result: map[string]string{"a|b|c": `{"key1":"a","key2":"b","key3":"c","time":"2018-02-01T01:01:01Z"}`},
		},
		{
			msg: "Invalid time field",
			worker: &Worker{
				Key:       []string{"key"},
				TimeField: "t",
			},
			data: []string{
				`{"key":"a"}`,
			},
			shouldErr: true,
		},
	}
	for _, test := range cases {
		var errored bool
		for _, b := range test.data {
			err := test.worker.dedup([]byte(b))
			if err != nil {
				errored = true
			}
		}
		if errored != test.shouldErr {
			t.Errorf("FAIL: '%s' error mismatch", test.msg)
		} else if !cmp.Equal(test.worker.data, test.result) {
			t.Errorf("FAIL: '%s' %s", test.msg, cmp.Diff(test.worker.data, test.result))
		} else {
			t.Logf("PASS: %v", test.msg)
		}
	}
}

func defaultWorker() *Worker {
	return &Worker{
		Key:       []string{"key"},
		TimeField: "time",
		Keep:      Newest,
		data:      make(map[string]string),
	}
}
