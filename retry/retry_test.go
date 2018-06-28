package retry

import (
	"errors"
	"testing"

	"fmt"

	"time"

	"github.com/jbsmith7741/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/bus/nop"
)

func TestNew(t *testing.T) {
	fn := func(args ...interface{}) (interface{}, error) {
		_, err := New(args[0].(*Options))
		return nil, err
	}
	trial.New(fn, trial.Cases{
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
	fn := func(args ...interface{}) (expected interface{}, err error) {
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
		for _, v := range args {
			tsk, ok := v.(*task.Task)
			if !ok {
				return nil, fmt.Errorf("%v is not of type *task.Task", v)
			}
			r.applyRule(tsk)
		}
		time.Sleep(time.Millisecond)
		return r.producer, nil
	}
	trial.New(fn, trial.Cases{
		"no errors - do nothing": {
			Input: &task.Task{
				Type:   "test",
				Result: task.CompleteResult,
			},
			Expected: map[string][]string{},
		},
		"retry a failed message": {
			Input: &task.Task{
				Type:   "test",
				Result: task.ErrResult,
			},
			Expected: map[string][]string{"test": {`{"type":"test"`}},
		},
		"success after 1st retry": {
			Input: trial.Args(
				&task.Task{Type: "test", Result: task.ErrResult},
				&task.Task{Type: "test", Result: task.CompleteResult},
			),
			Expected: map[string][]string{"test": {`{"type":"test"`}},
		},
		"just keeps failing": {
			Input: trial.Args(
				&task.Task{Type: "test", Result: task.ErrResult},
				&task.Task{Type: "test", Result: task.ErrResult},
				&task.Task{Type: "test", Result: task.ErrResult},
				&task.Task{Type: "test", Result: task.ErrResult},
				&task.Task{Type: "test", Result: task.ErrResult},
			),
			Expected: map[string][]string{
				"test":         {`{"type":"test"`, `{"type":"test"`, `{"type":"test"`, `{"type":"test"`},
				"retry-failed": {`{"type":"test"`, `{"type":"test"`}},
		},
	}).EqualFn(equalFn).Test(t)
}

func equalFn(actual interface{}, expected interface{}) bool {
	p := actual.(*nop.Producer)

	for topic, msgs := range expected.(map[string][]string) {
		for _, v := range msgs {
			if !p.Contains(topic, []byte(v)) {
				return false
			}
		}
	}
	return true
}
