package dedup

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file/mock"
)

func TestNewWorker(t *testing.T) {
	cases := []struct {
		msg       string
		info      string
		shouldErr bool
	}{
		{
			msg:  "Valid Worker",
			info: "?Key=key&TimeField=time&WritePath=nop://",
		},
		{
			msg:       "Invalid Worker - Bad URI",
			info:      "://",
			shouldErr: true,
		},
		{
			msg:       "Invalid Worker - bad reader",
			info:      "nop://init_err/path/file.txt?WritePath=nop",
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

func TestWorker_DoTask(t *testing.T) {
	cases := []struct {
		msg      string
		worker   *Worker
		result   task.Result
		expected string
		cancel   time.Duration
	}{
		{
			msg: "Good Path",
			worker: &Worker{
				Key:       []string{"key"},
				TimeField: "time",
				data:      make(map[string]string),
				reader:    mock.NewReader("", []string{`{"key":"a","time":"2018-01-02T12:00:00Z"}`, `{"key":"b","time":"2018-01-02T12:00:00Z"}`}, 2),
				writer:    mock.NewWriter("nop://", 0),
			},
			result:   task.CompleteResult,
			expected: "lines written: 2",
		},
		{
			msg: "Invalid write line",
			worker: &Worker{
				Key:       []string{"key"},
				TimeField: "time",
				data:      make(map[string]string),
				reader:    mock.NewReader("", []string{`{"key":"a","time":"2018-01-02T12:00:00Z"}`}, 10),
				writer:    mock.NewWriter("nop://writeline_err", 0),
			},
			result:   task.ErrResult,
			expected: "writeline_err",
		},
		{
			msg: "Cancel context in write loop",
			worker: &Worker{
				Key:       []string{"time"},
				TimeField: "time",
				data:      genData("mock data", 100),
				reader:    mock.NewReader("", nil, 0),
				writer:    mock.NewWriter("nop://", time.Millisecond),
			},
			result:   task.ErrResult,
			cancel:   1,
			expected: "task interrupted",
		},
		{
			msg: "Invalid read line",
			worker: &Worker{
				Key:       []string{"time"},
				TimeField: "time",
				data:      make(map[string]string),
				reader:    mock.NewReader("nop://readline_err", nil, 0),
				writer:    mock.NewWriter("", 0),
			},
			result: task.ErrResult,
		},
		{
			msg: "Fail on Dedup (invalid data)",
			worker: &Worker{
				Key:       []string{"time"},
				TimeField: "time",
				data:      make(map[string]string),
				reader:    mock.NewReader("nop://", []string{"mock"}, 1),
				writer:    mock.NewWriter("", 0),
			},
			result: task.ErrResult,
		},
		{
			msg: "Cancel context in read loop",
			worker: &Worker{
				Key:       []string{"time"},
				TimeField: "time",
				data:      make(map[string]string),
				reader:    mock.NewReader("nop://", []string{`{"key":"a","time":"2018-01-15T12:00:00Z"}`}, 100000),
				writer:    mock.NewWriter("", 0),
			},
			result: task.ErrResult,
			cancel: -1,
		},
	}
	for _, test := range cases {
		ctx, cancelfn := context.WithCancel(context.Background())
		if test.cancel != 0 {
			go func() {
				time.Sleep(test.cancel)
				cancelfn()
			}()
		}
		r, s := test.worker.DoTask(ctx)

		if r != test.result {
			t.Errorf("FAIL: %s %s %s", test.msg, cmp.Diff(r, test.result), s)
		} else if test.expected != "" && s != test.expected {
			t.Errorf("FAIL: %s %s", test.msg, cmp.Diff(s, test.expected))
		} else {
			t.Logf("PASS: %s %s", test.msg, s)
		}
	}
}

func genData(line string, n int) map[string]string {
	m := make(map[string]string)
	for i := 0; i < n; i++ {
		m[strconv.Itoa(i)] = line
	}
	return m
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
