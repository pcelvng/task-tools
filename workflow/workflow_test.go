package workflow

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func TestLoadFile(t *testing.T) {
	type input struct {
		path  string
		cache *Cache
	}
	fn := func(v trial.Input) (interface{}, error) {
		in := v.Interface().(input)
		if in.cache == nil {
			in.cache = &Cache{Workflows: make(map[string]Record)}
		}
		err := in.cache.loadFile(in.path, nil)
		_, f := filepath.Split(in.path)
		return in.cache.Workflows[f].Checksum, err
	}
	cases := trial.Cases{
		"read file": {
			Input:    input{path: "../internal/test/workflow/f1.toml"},
			Expected: "e841d7e6deb3e774caf3681034e1151a", // checksum of test file
		},
		"stat error": {
			Input:       input{path: "nop://stat_err"},
			ExpectedErr: errors.New("nop stat error"),
		},
		"dir error": {
			Input:       input{path: "nop://stat_dir"},
			ExpectedErr: errors.New("can not read directory"),
		},
		"read err": {
			Input:       input{path: "nop://init_err"},
			ExpectedErr: errors.New("new reader"),
		},
		"decode error": {
			Input:       input{path: "../internal/test/invalid.toml"},
			ExpectedErr: errors.New("decode"),
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestRefresh(t *testing.T) {
	fn := func(input trial.Input) (interface{}, error) {
		c := input.Interface().(*Cache)
		c.Workflows = make(map[string]Record)
		err := c.Refresh()
		return len(c.Workflows), err
	}
	cases := trial.Cases{
		"single file": {
			Input:    &Cache{path: "../internal/test/workflow/f1.toml"},
			Expected: 1, // load 1 file
		},
		"folder": {
			Input:    &Cache{path: "../internal/test/workflow", isDir: true},
			Expected: 2, // load folder with 2 files
		},
		"error case": {
			Input:     &Cache{path: "nop://err", isDir: true},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestNew(t *testing.T) {
	// error on new
	if _, err := New("nop://err", nil); err == nil {
		t.Error("Expected error")
	}

	//proper setup
	c, err := New("../internal/test/workflow/f1.toml", nil)
	if err != nil {
		t.Error("Unexpected error", err)
	}
	c.Close()
}

func TestGet(t *testing.T) {
	cache := &Cache{Workflows: map[string]Record{
		"workflow.toml": {
			Workflow: []Workflow{
				{Task: "task1"},
				{Task: "task2", DependsOn: "task1"},
				{Task: "task3", DependsOn: "task2"},
				{Task: "task4", DependsOn: "task2"},
			},
		},
	}}
	fn := func(v trial.Input) (interface{}, error) {
		t := v.Interface().(task.Task)
		return cache.Get(t), nil
	}
	cases := trial.Cases{
		"no meta": {
			Input:    task.Task{Type: "task1"},
			Expected: Workflow{},
		},
		"blank task": {
			Input:    task.Task{Meta: "workflow=workflow.toml"},
			Expected: Workflow{},
		},
		"not found": {
			Input:    task.Task{Type: "missing", Meta: "workflow=workflow.toml"},
			Expected: Workflow{},
		},
		"task2": {
			Input:    task.Task{Type: "task2", Meta: "workflow=workflow.toml"},
			Expected: Workflow{Task: "task2", DependsOn: "task1"},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestParent(t *testing.T) {
	cache := &Cache{Workflows: map[string]Record{
		"workflow.toml": {
			Workflow: []Workflow{
				{Task: "task1"},
				{Task: "task2", DependsOn: "task1"},
				{Task: "task3"},
				{Task: "task4", DependsOn: "task2"},
			},
		},
	}}
	w := cache.Workflows["workflow.toml"].Parent()
	if eq, s := trial.Equal(w, []Workflow{{Task: "task1"}, {Task: "task3"}}); !eq {
		t.Error("FAIL", s)
	}
}

func TestChildren(t *testing.T) {
	cache := &Cache{Workflows: map[string]Record{
		"workflow.toml": {
			Workflow: []Workflow{
				{Task: "task1"},
				{Task: "task2", DependsOn: "task1"},
				{Task: "task3", DependsOn: "task2"},
				{Task: "task4", DependsOn: "task2"},
			},
		},
	}}
	fn := func(v trial.Input) (interface{}, error) {
		t := v.Interface().(task.Task)
		return cache.Children(t), nil
	}
	cases := trial.Cases{
		"no meta": {
			Input:     task.Task{Type: "task1"},
			ShouldErr: true,
		},
		"blank task": {
			Input:     task.Task{Meta: "workflow=workflow.toml"},
			ShouldErr: true,
		},
		"task1": {
			Input:    task.Task{Type: "task1", Meta: "workflow=workflow.toml"},
			Expected: []Workflow{{Task: "task2", DependsOn: "task1"}},
		},
		"task2": {
			Input:    task.Task{Type: "task2", Meta: "workflow=workflow.toml"},
			Expected: []Workflow{{Task: "task3", DependsOn: "task2"}, {Task: "task4", DependsOn: "task2"}},
		},
		"task3": {
			Input:    task.Task{Type: "task3", Meta: "workflow=workflow.toml"},
			Expected: []Workflow{},
		},
		"task4": {
			Input:    task.Task{Type: "task4", Meta: "workflow=workflow.toml"},
			Expected: []Workflow{},
		},
	}
	trial.New(fn, cases).SubTest(t)
}
