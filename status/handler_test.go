package status

import (
	"testing"

	"github.com/jbsmith7741/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus/info"
	"github.com/pcelvng/task/bus/nop"
)

func TestHandler_AddFunc(t *testing.T) {
	fn := func(args ...interface{}) (interface{}, error) {
		h := New(0)
		err := h.AddFunc(args[0])
		return len(h.genericFn) == 1, err
	}
	trial.New(fn, trial.Cases{
		"Launcher stats func": {
			Input:    (&task.Launcher{}).Stats,
			Expected: true,
		},
		"nop consumer func": {
			Input:    (&nop.Consumer{}).Info,
			Expected: true,
		},
		"nop producer func": {
			Input:    (&nop.Producer{}).Info,
			Expected: true,
		},
		"generic func": {
			Input:    func() interface{} { return nil },
			Expected: true,
		},
		"invalid func": {
			Input:     func() {},
			ShouldErr: true,
		},
	}).Test(t)
}

func TestHandler_Compile(t *testing.T) {
	fn := func(args ...interface{}) (interface{}, error) {
		h := New(0)
		err := h.AddFunc(args[0])
		return h.Compile(), err
	}
	trial.New(fn, trial.Cases{
		"consumer data": {
			Input:    (&nop.Consumer{}).Info,
			Expected: map[string]interface{}{"consumer": info.Consumer{}},
		},
		"producer data": {
			Input:    (&nop.Producer{}).Info,
			Expected: map[string]interface{}{"producer": info.Producer{}},
		},
		"launcher data": {
			Input: func() task.LauncherStats {
				return task.LauncherStats{
					RunTime: "10s",
				}
			},
			Expected: map[string]interface{}{"launcher": task.LauncherStats{RunTime: "10s"}},
		},
		"generic data": {
			Input: func() interface{} {
				return struct {
					Int   int
					Value string
				}{Int: 11, Value: "Hello world"}
			},
			Expected: map[string]interface{}{"Int": 11, "Value": "Hello world"},
		},
	}).Test(t)
}
