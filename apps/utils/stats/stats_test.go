package main

import (
	"errors"
	"io/ioutil"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jbsmith7741/trial"
	"github.com/nsqio/go-nsq"
	"github.com/pcelvng/task"
)

func TestStat_DoneTask(t *testing.T) {
	type input struct {
		Stat  *stat
		Tasks []task.Task
	}
	type output struct {
		inProgress int
		failed     int64
		success    int64
	}
	fn := func(args ...interface{}) (interface{}, error) {
		in := args[0].(input)
		for _, t := range in.Tasks {
			(in.Stat).DoneTask(t)
		}
		return output{
			inProgress: len(in.Stat.inProgress),
			failed:     in.Stat.error.count,
			success:    in.Stat.success.count,
		}, nil
	}
	cases := trial.Cases{
		"remove inprogress task": {
			Input: input{
				Stat:  testStat(task.Task{Type: "a", Info: "?info", Created: "2018-01-01T00:00:00Z"}),
				Tasks: []task.Task{{Type: "a", Info: "?info", Created: "2018-01-01T00:00:00Z", Result: task.CompleteResult}},
			},
			Expected: output{
				success: 1,
			},
		},
		"ignore if not found": {
			Input: input{
				Stat:  testStat(task.Task{Type: "a", Info: "?info", Created: "2018-01-01T00:00:00Z"}),
				Tasks: []task.Task{{Type: "b", Info: "?info", Created: "2018-01-01T00:00:00Z"}},
			},
			Expected: output{
				inProgress: 1,
			},
		},
		"ignore if no result": {
			Input: input{
				Stat:  testStat(task.Task{Type: "a", Info: "?info", Created: "2018-01-01T00:00:00Z"}),
				Tasks: []task.Task{{Type: "a", Info: "?info", Created: "2018-01-01T00:00:00Z"}},
			},
			Expected: output{},
		},
		"some succeed some fail": {
			Input: input{
				Stat: testStat(
					task.Task{Type: "a", Info: "?info", Created: "2018-01-01T00:00:00Z"},
					task.Task{Type: "a", Info: "?info=2", Created: "2018-01-01T00:00:00Z"},
					task.Task{Type: "a", Info: "?info=3", Created: "2018-01-01T00:00:00Z"},
					task.Task{Type: "a", Info: "?info=4", Created: "2018-01-01T00:00:00Z"},
					task.Task{Type: "a", Info: "?info=5", Created: "2018-01-01T00:00:00Z"},
				),
				Tasks: []task.Task{
					{Type: "a", Info: "?info", Created: "2018-01-01T00:00:00Z", Result: task.CompleteResult},
					{Type: "a", Info: "?info=5", Created: "2018-01-01T00:00:00Z", Result: task.ErrResult},
					{Type: "a", Info: "?info=2", Created: "2018-01-01T00:00:00Z", Result: task.ErrResult},
				},
			},
			Expected: output{
				inProgress: 2,
				success:    1,
				failed:     2,
			},
		},
	}
	trial.New(fn, cases).Test(t)
}

func testStat(tasks ...task.Task) *stat {
	s := newStat(nil)
	for _, t := range tasks {
		s.inProgress[key(t)] = t
	}
	return s
}

func TestStat_HandleMessage(t *testing.T) {
	dummyID := nsq.MessageID{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'}
	fn := func(args ...interface{}) (interface{}, error) {
		b := args[0].(string)
		msg := nsq.NewMessage(dummyID, []byte(b))
		c := trial.CaptureLog()

		newStat(nil).HandleMessage(msg)
		if s := c.ReadAll(); s != "" {
			return nil, errors.New(s)
		}
		return nil, nil
	}
	cases := trial.Cases{
		"Bad Json": {
			Input:       "asde",
			ExpectedErr: errors.New("invalid task invalid character"),
		},
		"invalid end time": {
			Input:       `{"ended":"invalid"}`,
			ExpectedErr: errors.New("invalid task parsing time"),
		},
		"valid task": {
			Input: `{"type":"task","info":"","created":"2018-11-10T00:00:00Z","result":"complete","started":"2018-11-10T00:00:00Z","ended":"2018-11-10T00:00:00Z"}`,
		},
	}
	trial.New(fn, cases).Test(t)
}

func TestApp_HandleMessage(t *testing.T) {
	// setup test data
	a := &app{
		topics: map[string]*stat{
			"task1": {
				inProgress: map[string]task.Task{
					"1:2:3": {},
					"2:2:2": {},
				},
				success: &durStats{
					count: 10,
					sum:   10,
					Min:   0,
					Max:   time.Second,
				},
				error: &durStats{},
			},
			"task2": {
				inProgress: make(map[string]task.Task),
				success: &durStats{
					count: 3,
					sum:   250,
					Min:   5 * time.Millisecond,
					Max:   10 * time.Second,
				},
				error: &durStats{
					count: 1,
					sum:   5,
					Min:   time.Microsecond,
					Max:   3 * time.Second,
				},
			},
			"task3": {
				inProgress: make(map[string]task.Task),
				success:    &durStats{},
				error: &durStats{
					count: 7,
					sum:   385,
					Min:   50 * time.Millisecond,
					Max:   100 * time.Millisecond,
				},
			},
		},
	}
	response := []string{`task1
Success: 1.00% 	10  min: 0s max 1s avg:10ms
Failed: 0.00% 
InProgress: 2`, `task2
Success: 0.75% 	3  min: 5ms max 10s avg:830ms
Failed: 0.25% 	1  min: 1µs max 3s avg:50ms
InProgress: 0`, `task3
Success: 0.00% 
Failed: 1.00% 	7  min: 50ms max 100ms avg:550ms
InProgress: 0`}
	fn := func(args ...interface{}) (interface{}, error) {
		req := httptest.NewRequest("GET", args[0].(string), nil)
		w := httptest.NewRecorder()
		a.handler(w, req)
		b, err := ioutil.ReadAll(w.Body)
		if err != nil {
			return nil, err
		}
		// remove first line uptime from result
		s := string(b)
		s = s[strings.Index(s, "\n")+1:]
		return s, nil
	}
	cases := trial.Cases{
		"all topics": {
			Input:    "localhost:8080?",
			Expected: strings.Join(response, "\n\n") + "\n\n",
		},
		"task1": {
			Input:    "localhost:8080?topic=task1",
			Expected: response[0] + "\n\n",
		},
		"task1 & task2": {
			Input:    "localhost:8080?topic=task1&topic=task2",
			Expected: response[0] + "\n\n" + response[1] + "\n\n",
		},
		"task1,task3": {
			Input:    "localhost:8080?topic=task1,task3",
			Expected: response[0] + "\n\n" + response[2] + "\n\n",
		},
		"invalid task": {
			Input:    "localhost:8080?topic=invalid",
			Expected: "",
		},
	}
	trial.New(fn, cases).Test(t)
}
