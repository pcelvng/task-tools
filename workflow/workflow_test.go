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
			in.cache = &Cache{Workflows: make(map[string]Workflow)}
		}
		_, err := in.cache.loadFile(in.path, nil)
		f := in.cache.filePath(in.path)
		return in.cache.Workflows[f].Checksum, err
	}
	cases := trial.Cases{
		"read file": {
			Input:    input{path: "../internal/test/workflow/f1.toml"},
			Expected: "4422274d9c9f7e987c609687a7702651", // checksum of test file
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
		if c.Workflows == nil {
			c.Workflows = make(map[string]Workflow)
		}
		_, err := c.Refresh()
		return len(c.Workflows), err
	}
	cases := trial.Cases{
		"single file": {
			Input:    &Cache{path: "../internal/test/workflow/f1.toml"},
			Expected: 1, // load 1 file
		},
		"folder": {
			Input:    &Cache{path: "../internal/test/workflow", isDir: true},
			Expected: 3, // load folder with 2 files
		},
		"sub-folder": {
			Input:    &Cache{path: "../internal/test/parent", isDir: true},
			Expected: 2, // load folder with 1 files and sub-folder with 1 file
		},
		"error case": {
			Input:     &Cache{path: "nop://err", isDir: true},
			ShouldErr: true,
		},
		"file removed": {
			Input: &Cache{
				path:  "../internal/test/workflow",
				isDir: true,
				Workflows: map[string]Workflow{
					"missing.toml": {},
					"f1.toml":      {},
					"f2.toml":      {},
					"f3.toml":      {},
				},
			},
			Expected: 3,
		},
		"keep loaded": {
			Input: &Cache{
				path:  testPath("../internal/test/workflow"),
				isDir: true,
				Workflows: map[string]Workflow{
					"f1.toml": {
						Checksum: "34cf5142fbd029fa778ee657592d03ce",
					},
					"f2.toml": {
						Checksum: "eac7716a13d9dea0d630c5d8b1e6c6b1",
					},
				},
			},
			Expected: 3,
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func testPath(s string) string {
	st, _ := filepath.Abs(s)
	return st
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

	// workflow invalid or empty
	if _, err := New("../internal/test/workflow/missing.toml", nil); err == nil {
		t.Error("Expected error for missing file")
	}

}

func TestGet(t *testing.T) {
	cache := &Cache{Workflows: map[string]Workflow{
		"workflow.toml": {
			Phases: []Phase{
				{Task: "task1"},
				{Task: "dup"},
				{Task: "task2", DependsOn: "task1"},
				{Task: "task3", DependsOn: "task2"},
				{Task: "task4", DependsOn: "task2"},
			},
		},
		"w2job.toml": {
			Phases: []Phase{
				{Task: "dup"},
				{Task: "t2", Rule: "job=j1"},
				{Task: "t2", Rule: "job=j2"},
				{Task: "t2", Rule: "job=j3"},
			},
		},
	},
	}
	fn := func(v trial.Input) (interface{}, error) {
		t := v.Interface().(task.Task)
		return cache.Get(t), nil
	}
	cases := trial.Cases{
		"no meta": {
			Input:    task.Task{Type: "task1"},
			Expected: Phase{},
		},
		"blank task": {
			Input:    task.Task{Meta: "workflow=workflow.toml"},
			Expected: Phase{},
		},
		"not found": {
			Input:    task.Task{Type: "missing", Meta: "workflow=workflow.toml"},
			Expected: Phase{},
		},
		"task2": {
			Input:    task.Task{Type: "task2", Meta: "workflow=workflow.toml"},
			Expected: Phase{Task: "task2", DependsOn: "task1"},
		},
		"task=t2 with job=j1": {
			Input:    task.Task{Type: "t2", Meta: "workflow=w2job.toml&job=j1"},
			Expected: Phase{Task: "t2", Rule: "job=j1"},
		},
		"job does not exist": {
			Input:    task.Task{Type: "t2", Meta: "workflow=w2job.toml&job=invalid"},
			Expected: Phase{},
		},
		"wildcard search": {
			Input:    task.Task{Type: "t2", Meta: "workflow=*&job=j3"},
			Expected: Phase{Task: "t2", Rule: "job=j3"},
		},
		"wildcard with same task in different files": { // picks first match, results will vary
			Input:    task.Task{Type: "dup", Meta: "workflow=*"},
			Expected: Phase{Task: "dup"},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestParent(t *testing.T) {
	cache := &Cache{Workflows: map[string]Workflow{
		"workflow.toml": {
			Phases: []Phase{
				{Task: "task1"},
				{Task: "task2", DependsOn: "task1"},
				{Task: "task3"},
				{Task: "task4", DependsOn: "task2"},
			},
		},
	}}
	w := cache.Workflows["workflow.toml"].Parent()
	if eq, s := trial.Equal(w, []Phase{{Task: "task1"}, {Task: "task3"}}); !eq {
		t.Error("FAIL", s)
	}
}

func TestChildren(t *testing.T) {
	cache := &Cache{Workflows: map[string]Workflow{
		"workflow.toml": {
			Phases: []Phase{
				{Task: "task1"},
				{Task: "task2", DependsOn: "task1"},
				{Task: "task3", DependsOn: "task2"},
				{Task: "task4", DependsOn: "task2"},
				{Task: "task5", DependsOn: "task1:j4"},
			},
		},
	}}
	fn := func(v trial.Input) (interface{}, error) {
		t := v.Interface().(task.Task)
		return cache.Children(t), nil
	}
	cases := trial.Cases{
		"no meta": {
			Input:    task.Task{Type: "task1"},
			Expected: []Phase(nil),
		},
		"blank task": {
			Input:    task.Task{Meta: "workflow=workflow.toml"},
			Expected: []Phase(nil),
		},
		"task1": {
			Input:    task.Task{Type: "task1", Meta: "workflow=workflow.toml"},
			Expected: []Phase{{Task: "task2", DependsOn: "task1"}},
		},
		"task2": {
			Input:    task.Task{Type: "task2", Meta: "workflow=workflow.toml"},
			Expected: []Phase{{Task: "task3", DependsOn: "task2"}, {Task: "task4", DependsOn: "task2"}},
		},
		"task3": {
			Input:    task.Task{Type: "task3", Meta: "workflow=workflow.toml"},
			Expected: []Phase{},
		},
		"task4": {
			Input:    task.Task{Type: "task4", Meta: "workflow=workflow.toml"},
			Expected: []Phase{},
		},
		"task1:j4": {
			Input: task.Task{Type: "task1", Meta: "workflow=workflow.toml&job=j4"},
			Expected: []Phase{
				{Task: "task2", DependsOn: "task1"},
				{Task: "task5", DependsOn: "task1:j4"},
			},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestCache_FilePath(t *testing.T) {

	type input struct {
		cachePath string
		file      string
	}

	fn := func(v trial.Input) (interface{}, error) {

		c := &Cache{path: v.Slice(0).String()}
		return c.filePath(v.Slice(1).String()), nil
	}
	cases := trial.Cases{
		"single file": {
			Input:    []string{"./path", "./path/file.toml"},
			Expected: "file.toml",
		},
		"same name": {
			Input:    []string{"./path/file.toml", "./path/file.toml"},
			Expected: "file.toml",
		},
		"sub directory": {
			Input:    []string{"./path", "./path/sub/file.toml"},
			Expected: "sub/file.toml",
		},
		"embedded": {
			Input:    []string{"./path", "root/folder/path/file.toml"},
			Expected: "file.toml",
		},
		"embedded sub": {
			Input:    []string{"./path", "root/path/sub/file.toml"},
			Expected: "sub/file.toml",
		},
	}
	trial.New(fn, cases).SubTest(t)
}
