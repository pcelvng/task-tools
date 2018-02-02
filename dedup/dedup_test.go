package dedup

import (
	"context"
	"strconv"
	"testing"
	"time"

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
			msg:      "Good Path",
			worker:   testWorker("nop://readline_eof?WritePath=nop://&TimeField=time", `{"key""time":"2018-01-02T12:00:00Z"}`, 10),
			result:   task.CompleteResult,
			expected: "Lines written: 10",
		},
		{
			msg:      "Invalid write line",
			worker:   testWorker("nop://readline_eof?WritePath=nop://writeline_err/other/fake/path.txt", "Random data", 100),
			result:   task.ErrResult,
			expected: "writeline_err",
		},
		{
			msg:      "Cancel context in write loop",
			worker:   testWorker("nop://readline_eof?WritePath=nop://", "Random data", 10000),
			result:   task.ErrResult,
			cancel:   1,
			expected: "task interrupted",
		},
		{
			msg:    "Invalid read line",
			worker: mockReadWorker("nop://readline_err?WritePath=nop://", []string{"mock"}, 1),
			result: task.ErrResult,
		},
		{
			msg:    "Fail on Dedup (invalid data)",
			worker: mockReadWorker("nop://host/path.txt?WritePath=nop://", []string{"mock"}, 1),
			result: task.ErrResult,
		},
		{
			msg:    "Cancel context in read loop",
			worker: mockReadWorker("nop://host/path.txt?WritePath=nop://&Key=key&TimeField=time", []string{`{"key":"a","time":"2018-01-15T12:00:00Z"}`}, 1000),
			result: task.ErrResult,
			cancel: time.Nanosecond,
		},
	}
	for _, test := range cases {
		ctx, cancelfn := context.WithCancel(context.Background())
		if test.cancel > 0 {
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

func testWorker(uri string, line string, count int) *Worker {
	w := (&Config{}).NewWorker(uri)
	if invalid, msg := task.IsInvalidWorker(w); invalid {
		panic(msg)
	}
	worker := w.(*Worker)
	if count > 0 {
		for i := 0; i < count; i++ {
			worker.data[strconv.Itoa(i)] = line
		}
	}
	return worker
}

func mockReadWorker(uri string, data []string, count int) *Worker {
	w := (&Config{}).NewWorker(uri)
	if invalid, msg := task.IsInvalidWorker(w); invalid {
		panic(msg)
	}
	worker := w.(*Worker)
	reader, _ := newMockReader(worker.ReadPath)
	reader.Lines = data
	reader.LineCount = count
	worker.reader = reader
	return worker
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
