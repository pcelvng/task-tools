package cache

import (
	"errors"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/hydronica/toml"
	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func MockSQLite() *SQLite {
	s := &SQLite{LocalPath: ":memory:"}
	if err := s.initDB(); err != nil {
		panic(err)
	}
	return s
}

const testPath = "../../../internal/test/"

func TestLoadFile(t *testing.T) {
	fn := func(in string) (string, error) {
		cache := MockSQLite()
		_, err := cache.loadFile(in, nil)
		return cache.getFileHash(in), err
	}
	cases := trial.Cases[string, string]{
		"read file": {
			Input:    testPath + "workflow/f1.toml",
			Expected: "4422274d9c9f7e987c609687a7702651", // checksum of test file
		},
		"stat error": {
			Input:       "nop://stat_err",
			ExpectedErr: errors.New("nop stat error"),
		},
		"dir error": {
			Input:       "nop://stat_dir",
			ExpectedErr: errors.New("can not read directory"),
		},
		"read err": {
			Input:       "nop://init_err",
			ExpectedErr: errors.New("new reader"),
		},
		"decode error": {
			Input:       testPath + "invalid.toml",
			ExpectedErr: errors.New("decode"),
		},
	}
	trial.New(fn, cases).SubTest(t)
}

// TestLoadPhase is used to validated that a phase is correctly loaded into the DB
func TestValidatePhase(t *testing.T) {
	fn := func(ph Phase) (string, error) {
		s := ph.Validate()
		if s != "" {
			return "", errors.New(s)
		}
		return "", nil
	}
	cases := trial.Cases[Phase, string]{
		"Ok": {
			Input: Phase{Rule: "files=s3:/bucket/path/*/*.txt"},
		},
		"empty phase": {
			Input:       Phase{},
			ExpectedErr: errors.New("non-scheduled phase"),
		},
		"invalid cron": {
			Input:       Phase{Rule: "cron=abcdefg"},
			ExpectedErr: errors.New("invalid cron"),
		},
		"cron 5": {
			Input: Phase{Rule: "cron=1 2 3 4 5"},
		},
		"cron 6": {
			Input: Phase{Rule: "cron=1 2 3 4 5 6"},
		},
		"cron complex": 
		{
			Input: Phase{Rule: "cron=20 */6 * * SUN"},
		},
		"parse_err": {
			Input:       Phase{Rule: "a;lskdfj?%$`?\"^"},
			ExpectedErr: errors.New("invalid rule format"),
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestToml(t *testing.T) {
	v := `
[[phase]]
task = "task1"
rule = "cron=0 * * * *&offset=-4h&job=t2&retry_delay=10ms"
retry = 3
template = "?date={yyyy}-{mm}-{dd}T{hh}"

[[phase]]
task = "task2"
dependsOn = "task1"
rule = ""
retry = 3
template = "{meta:file}?time={yyyy}-{mm}-{dd}"
`
	w := &Workflow{}

	if _, err := toml.Decode(v, w); err != nil {
		t.Fatalf(err.Error())
	}
	if len(w.Phases) != 2 {
		t.Errorf("Expected 2 phases got %d", len(w.Phases))
		t.Log(spew.Sdump(w.Phases))
	}

}

func TestRefresh(t *testing.T) {
	type Cache struct {
		path      string
		isDir     bool
		Workflows map[string]Workflow
	}

	fn := func(c *Cache) (int, error) {
		sqlite := MockSQLite()
		for path, workflow := range c.Workflows {
			if err := sqlite.updateWorkflowInDB(path, workflow.Checksum, workflow.Phases); err != nil {
				return 0, err
			}
		}
		sqlite.workflowPath = c.path
		sqlite.isDir = c.isDir
		_, err := sqlite.Refresh()
		return len(sqlite.GetWorkflowFiles()), err
	}
	cases := trial.Cases[*Cache, int]{
		"single file": {
			Input:    &Cache{path: testPath + "workflow/f1.toml"},
			Expected: 1, // load 1 file
		},
		"folder": {
			Input:    &Cache{path: testPath + "workflow", isDir: true},
			Expected: 4, // load folder with 2 files
		},
		"sub-folder": {
			Input:    &Cache{path: testPath + "parent", isDir: true},
			Expected: 2, // load folder with 1 files and sub-folder with 1 file
		},
		"error case": {
			Input:     &Cache{path: "nop://err", isDir: true},
			ShouldErr: true,
		},
		"file removed": {
			Input: &Cache{
				path:  testPath + "workflow",
				isDir: true,
				Workflows: map[string]Workflow{
					"missing.toml": {},
					"f1.toml":      {},
					"f2.toml":      {},
					"f3.toml":      {},
				},
			},
			Expected: 4,
		},
		"keep loaded": {
			Input: &Cache{
				path:  testPath + "workflow",
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
			Expected: 4,
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestGet(t *testing.T) {
	cache := MockSQLite()
	err := cache.updateWorkflowInDB("workflow.toml", "NA", []Phase{
		{Task: "task1"},
		{Task: "dup"},
		{Task: "task2", DependsOn: "task1"},
		{Task: "task3", DependsOn: "task2"},
		{Task: "task4", DependsOn: "task2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = cache.updateWorkflowInDB("w2job.toml", "NA", []Phase{
		{Task: "dup"},
		{Task: "t2", Rule: "job=j1"},
		{Task: "t2", Rule: "job=j2"},
		{Task: "t2:j3", Rule: ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	fn := func(t task.Task) (Phase, error) {
		return cache.Get(t).Phase, nil
	}
	cases := trial.Cases[task.Task, Phase]{
		"no meta": {
			Input:    task.Task{Type: "task1"},
			Expected: Phase{Task: "task1"},
		},
		"blank task": {
			Input:    task.Task{Meta: "workflow=workflow.toml"},
			Expected: Phase{},
		},
		"not found": {
			Input:    task.Task{Type: "missing", Meta: "workflow=workflow.toml"},
			Expected: Phase{},
		},
		"task_job": {
			Input:    task.Task{Type: "t2", Job: "j2", Meta: "workflow=*"},
			Expected: Phase{Rule: "job=j2", Task: "t2:j2"},
		},
		"task2": {
			Input:    task.Task{Type: "task2", Meta: "workflow=workflow.toml"},
			Expected: Phase{Task: "task2", DependsOn: "task1"},
		},
		"task=t2 with job=j1": {
			Input:    task.Task{Type: "t2", Meta: "workflow=w2job.toml&job=j1"},
			Expected: Phase{Task: "t2:j1", Rule: "job=j1"},
		},
		"job does not exist": {
			Input:    task.Task{Type: "t2", Meta: "workflow=w2job.toml&job=invalid"},
			Expected: Phase{},
		},
		"wildcard search": {
			Input:    task.Task{Type: "t2", Meta: "workflow=*&job=j3"},
			Expected: Phase{Task: "t2:j3"},
		},
		"wildcard with same task in different files": { // picks first match, results will vary
			Input:    task.Task{Type: "dup", Meta: "workflow=*"},
			Expected: Phase{Task: "dup"},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestChildren(t *testing.T) {
	cache := MockSQLite()
	err := cache.updateWorkflowInDB("workflow.toml", "NA", []Phase{
		{Task: "task1"},
		{Task: "task2", DependsOn: "task1"},
		{Task: "task3", DependsOn: "task2"},
		{Task: "task4", DependsOn: "task2"},
		{Task: "task5", DependsOn: "task1:j4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	fn := func(t task.Task) ([]Phase, error) {
		return cache.Children(t), nil
	}
	cases := trial.Cases[task.Task, []Phase]{
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
	// TODO: remove receive struct as it is unneeded
	fn := func(v trial.Input) (string, error) {
		c := &SQLite{workflowPath: v.Slice(0).String()}
		return c.filePath(v.Slice(1).String()), nil
	}
	cases := trial.Cases[trial.Input, string]{
		"single file": {
			Input:    trial.Args("./path", "./path/file.toml"),
			Expected: "file.toml",
		},
		"same name": {
			Input:    trial.Args("./path/file.toml", "./path/file.toml"),
			Expected: "file.toml",
		},
		"sub directory": {
			Input:    trial.Args("./path", "./path/sub/file.toml"),
			Expected: "sub/file.toml",
		},
		"embedded": {
			Input:    trial.Args("./path", "root/folder/path/file.toml"),
			Expected: "file.toml",
		},
		"embedded sub": {
			Input:    trial.Args("./path", "root/path/sub/file.toml"),
			Expected: "sub/file.toml",
		},
	}
	trial.New(fn, cases).SubTest(t)
}
