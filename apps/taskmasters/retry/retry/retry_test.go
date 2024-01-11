package retry

import (
	"errors"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/bus/nop"
)

func TestNew(t *testing.T) {
	fn := func(opt *Options) (interface{}, error) {
		_, err := New(opt)
		return nil, err
	}
	trial.New(fn, trial.Cases[*Options, any]{
		"No rules": {
			Input:       &Options{},
			ExpectedErr: errors.New("no retry rules specified"),
		},
		"invalid consumer": {
			Input: &Options{
				RetryRules: []*RetryRule{{"test", 1, 0, ""}},
				Options:    bus.Options{Bus: "nop://init_err"},
			},
			ShouldErr: true,
		},
		"good path": {
			Input: &Options{
				RetryRules: []*RetryRule{{"test", 1, 0, ""}},
				Options:    bus.Options{Bus: "nop"},
			},
		},
	}).Test(t)
}

func TestApplyRule(t *testing.T) {
	fn := func(in []task.Task) (expected interface{}, err error) {
		r, err := New(&Options{
			Options: bus.Options{
				Bus: "nop",
			},
			RetriedTopic:     "retried",
			RetryFailedTopic: "retry-failed",
			RetryRules:       []*RetryRule{{"test", 2, 0, ""}},
		})
		if err != nil {
			return nil, err
		}
		for _, tsk := range in {
			r.applyRule(&tsk)
		}
		time.Sleep(time.Millisecond)
		return r.producer, nil
	}
	trial.New(fn, trial.Cases[[]task.Task, any]{
		"no errors - do nothing": {
			Input: []task.Task{
				{
					Type:   "test",
					Result: task.CompleteResult,
				},
			},
			Expected: map[string][]string{},
		},
		"retry a failed message": {
			Input: []task.Task{{
				Type:   "test",
				Result: task.ErrResult,
			}},
			Expected: map[string][]string{"test": {`{"type":"test"`}},
		},
		"success after 1st retry": {
			Input: []task.Task{
				{Type: "test", Result: task.ErrResult},
				{Type: "test", Result: task.CompleteResult},
			},
			Expected: map[string][]string{"test": {`{"type":"test"`}},
		},
		"just keeps failing": {
			Input: []task.Task{
				{Type: "test", Result: task.ErrResult},
				{Type: "test", Result: task.ErrResult},
				{Type: "test", Result: task.ErrResult},
				{Type: "test", Result: task.ErrResult},
				{Type: "test", Result: task.ErrResult},
			},
			Expected: map[string][]string{
				"test":         {`{"type":"test"`, `{"type":"test"`, `{"type":"test"`, `{"type":"test"`},
				"retry-failed": {`{"type":"test"`, `{"type":"test"`}},
		},
	}).EqualFn(equalFn).Test(t)
}

func equalFn(actual interface{}, expected interface{}) (bool, string) {
	p := actual.(*nop.Producer)

	for topic, msgs := range expected.(map[string][]string) {
		for _, v := range msgs {
			if !p.Contains(topic, []byte(v)) {
				return false, ""
			}
		}
	}
	return true, ""
}
