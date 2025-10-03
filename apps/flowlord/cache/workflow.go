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

// Workflow is a list of phases with a checksum for the file
type Workflow struct {
	Checksum string  // md5 hash for the file to check for changes
	Phases   []Phase `toml:"phase"`
}

type Cache struct{} // replace with sqlite db

// newWorkflow read the workflow file or directory and updates the underlying db cache
func newWorkflow(path, opts *file.Options) *Cache { return nil }

// Search
// Do we still need to return the path if its stored in a DB?
// should list return a list of matching phases rather than the first match?
func (c *Cache) Search(task, job string) (path string, ph Phase) { return "", Phase{} }

// Get Phase associated with task based on Task.Topic and Task.Job and workflow file
func (c *Cache) Get(t task.Task) Phase {
	return Phase{}
}

// Children of the given task t, a child phase is one that dependsOn another task
// Empty slice will be returned if no children are found.
// A task without a type or metadata containing the workflow info
// will result in an error
func (c *Cache) Children(t task.Task) []Phase { return nil }

// Refresh checks the cache and reloads any files if the checksum has changed.
func (c *Cache) Refresh() (changedFiles []string, err error) { return nil, nil }

// listAllFiles recursively lists all files in a folder and sub-folders
// Keep as is?
func listAllFiles(p string, opts *file.Options) ([]string, error) { return nil, nil }

// loadFile checks a files checksum and updates map if required
// loaded file name is returned
// Keep as is?
func (c *Cache) loadFile(path string, opts *file.Options) (f string, err error) { return "", nil }

// filePath returns a filePath consist of all unique part
// after the path set in the cache
// may not be needed if we store in a DB
func (c *Cache) filePath(p string) (s string) { return "" }

// Close the cache
// the chanel is used to force a wait until all routines are done
func (c *Cache) Close() error {
	//	close(c.done)
	return nil
}
