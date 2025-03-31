package bootstrap

import (
	"flag"
	"os"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

type mockOpts struct {
	validate bool
	Name     string `toml:"name"`
	Value    int    `toml:"value"`
}

func (u *mockOpts) Validate() error {
	u.validate = true // verify validate func runs
	return nil
}

func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
}

func TestUtilInit(t *testing.T) {
	resetFlags()
	// Set up command line args to use the config file
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"cmd", "-c", "tconf.toml"}

	// Initialize the utility with our mock
	v := &mockOpts{}
	util := NewUtility("mUtil", v).Version("1.0.0").Description("describe me").Initialize()

	// Verify Validate was called
	if !v.validate {
		t.Error("Validate was not called")
	}

	exp := &mockOpts{
		validate: true,
		Name:     "test-config",
		Value:    42,
	}
	if eq, diff := trial.Equal(exp, v); !eq {
		t.Error("FAIL: ", diff)
	} else {
		t.Log("PASS: config loaded")
	}
	// Verify utility name was set
	if util.name != "mUtil" {
		t.Errorf("Expected utility name to be 'mUtil', got '%s'", util.name)
	} else if util.description != "describe me" {
		t.Errorf("Expected Description 'describe me', got '%s'", util.description)
	} else if util.version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", util.version)
	} else {
		t.Log("PASS: Utils values updated")
	}
}

func TestWorkerInit(t *testing.T) {
	resetFlags()
	// Set up command line args to use the config file
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"cmd", "-c", "tconf.toml"}

	// Initialize the worker with our mock
	v := &mockOpts{}
	worker := NewWorkerApp("test-worker",
		func(info string) task.Worker { return nil },
		v).
		Version("1.0.0").
		Description("worker desc").
		Initialize()

	// Verify Validate was called
	if !v.validate {
		t.Error("Validate was not called")
	}

	exp := &mockOpts{
		validate: true,
		Name:     "test-config",
		Value:    42,
	}
	if eq, diff := trial.Equal(exp, v); !eq {
		t.Error("FAIL: ", diff)
	} else {
		t.Log("PASS: config loaded")
	}

	// Verify worker properties
	if worker.name != "test-worker" {
		t.Errorf("Expected worker name to be 'test-worker', got '%s'", worker.name)
	} else if worker.description != "worker desc" {
		t.Errorf("Expected Description 'worker desc', got '%s'", worker.description)
	} else if worker.version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", worker.version)
	} else if worker.bType != "worker" {
		t.Errorf("Expected bType 'worker', got '%s'", worker.bType)
	} else {
		t.Log("PASS: Worker values updated")
	}
}

func TestTaskMasterInit(t *testing.T) {
	resetFlags()
	// Set up command line args to use the config file
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"cmd", "-c", "tconf.toml"}

	// Initialize the taskmaster with our mock
	v := &mockOpts{}
	master := NewTaskMaster("test-master",
		func(s *Starter) Runner { return nil },
		v).
		Version("1.0.0").
		Description("master desc").
		Initialize()

	// Verify Validate was called
	if !v.validate {
		t.Error("Validate was not called")
	}

	exp := &mockOpts{
		validate: true,
		Name:     "test-config",
		Value:    42,
	}
	if eq, diff := trial.Equal(exp, v); !eq {
		t.Error("FAIL: ", diff)
	} else {
		t.Log("PASS: config loaded")
	}

	// Verify taskmaster properties
	if master.name != "test-master" {
		t.Errorf("Expected master name to be 'test-master', got '%s'", master.name)
	} else if master.description != "master desc" {
		t.Errorf("Expected Description 'master desc', got '%s'", master.description)
	} else if master.version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", master.version)
	} else if master.bType != "master" {
		t.Errorf("Expected bType 'master', got '%s'", master.bType)
	} else {
		t.Log("PASS: TaskMaster values updated")
	}
}
