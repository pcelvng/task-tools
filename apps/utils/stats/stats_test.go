package main

import (
	"errors"
	"testing"

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
