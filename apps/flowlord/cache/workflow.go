package cache

import (
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/file"
)

// Phase is same as workflow.Phase
type Phase struct {
	Task      string // Should use Topic() and Job() for access
	Rule      string
	DependsOn string // Task that the previous workflow depends on
	Retry     int
	Template  string // template used to create the task
}

func (p Phase) IsEmpty() bool { return false }
func (p Phase) Job() string   { return "" }
func (p Phase) Topic() string { return "" }

/*
// Workflow is a list of phases with a checksum for the file
type Workflow struct {
	Checksum string  // md5 hash for the file to check for changes
	Phases   []Phase `toml:"phase"`
}
*/

// newWorkflow read the workflow file or directory and updates the underlying db cache
func newWorkflow(path, opts *file.Options) *SQLite { return nil }

// Search
// Do we still need to return the path if it's stored in a DB?
// should list return a list of matching phases rather than the first match?
func (c *SQLite) Search(task, job string) (path string, ph Phase) { return "", Phase{} }

// GetPhase associated with task based on Task.Topic and Task.Job and workflow file
func (c *SQLite) GetPhase(t task.Task) Phase {
	return Phase{}
}

// Children of the given task t, a child phase is one that dependsOn another task
// Empty slice will be returned if no children are found.
// A task without a type or metadata containing the workflow info
// will result in an error
func (c *SQLite) Children(t task.Task) []Phase { return nil }

// Refresh checks the cache and reloads any files if the checksum has changed.
func (c *SQLite) Refresh() (changedFiles []string, err error) { return nil, nil }

// listAllFiles recursively lists all files in a folder and sub-folders
// Keep as is?
func listAllFiles(p string, opts *file.Options) ([]string, error) { return nil, nil }

// loadFile checks a files checksum and updates map if required
// loaded file name is returned
// Keep as is?
func (c *SQLite) loadFile(path string, opts *file.Options) (f string, err error) { return "", nil }

// filePath returns a filePath consist of all unique part
// after the path set in the cache
// may not be needed if we store in a DB
func (c *SQLite) filePath(p string) (s string) { return "" }
