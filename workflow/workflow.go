package workflow

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pcelvng/task"
	"github.com/pkg/errors"

	"github.com/pcelvng/task-tools/file"
)

type Workflow struct {
	Task      string // doubles as the Name of the topic to send data to
	Rule      string
	DependsOn string // Task that the previous workflow depends on
	Retry     int
	Template  string // template used to create the task
}

type Record struct {
	Checksum string
	Workflow []Workflow
}

type Cache struct {
	done  chan struct{}
	path  string
	isDir bool
	fOpts file.Options

	Workflows map[string]Record // the key is the filename for the workflow
}

// New returns a Cache used to manage auto updating a workflow
func New(path string, opts *file.Options) (*Cache, error) {
	c := &Cache{
		done:      make(chan struct{}),
		Workflows: make(map[string]Record),
		path:      path,
	}
	if opts != nil {
		c.fOpts = *opts
	}
	sts, err := file.Stat(path, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "problem with path %s", path)
	}
	c.isDir = sts.IsDir
	return c, c.Refresh()
}

// Parent workflow for the specified file.
// A parent workflow is one that doesn't depend on any other tasks
func (r Record) Parent() (p []Workflow) {
	for _, w := range r.Workflow {
		if w.DependsOn == "" {
			p = append(p, w)
		}
	}
	return p
}

// Get the Workflow associated with the task t
func (c *Cache) Get(t task.Task) Workflow {
	values, _ := url.ParseQuery(t.Meta)
	key := values.Get("workflow")
	for _, w := range c.Workflows[key].Workflow {
		if w.Task == t.Type {
			return w
		}
	}

	return Workflow{}
}

// Children of the given task t, a child workflow is one that dependsOn another task
// Empty slice will be returned if no children are found.
// A task without a type or meta data containing the workflow info
// will result in an error
func (c *Cache) Children(t task.Task) []Workflow {
	if t.Type == "" {
		return nil
	}
	values, _ := url.ParseQuery(t.Meta)
	result := make([]Workflow, 0)
	key := values.Get("workflow")
	if key == "" {
		return nil
	}
	for _, w := range c.Workflows[key].Workflow {
		if w.DependsOn == t.Type {
			result = append(result, w)
		}
	}
	return result
}

// Refresh checks the cache and reloads any files in the checksum has changed.
func (c *Cache) Refresh() error {
	if !c.isDir {
		return c.loadFile(c.path, &c.fOpts)
	}

	//list and read all files
	sts, err := file.List(c.path, &c.fOpts)
	if err != nil {
		return err
	}
	errs := appenderr.New()
	for _, s := range sts {
		errs.Add(c.loadFile(s.Path, &c.fOpts))
	}

	return errs.ErrOrNil()
}

// loadFile checks a files checksum and updates if required
func (c *Cache) loadFile(path string, opts *file.Options) error {
	_, f := filepath.Split(path)
	sts, err := file.Stat(path, opts)
	data := c.Workflows[f]
	// permission issues
	if err != nil {
		return errors.Wrapf(err, "stats %s", path)
	}
	// We can't process a directory here
	if sts.IsDir {
		return fmt.Errorf("can not read directory %s", path)
	}
	// check if file has changed
	if data.Checksum == sts.Checksum {
		return nil
	}
	data.Checksum = sts.Checksum

	r, err := file.NewReader(path, opts)
	if err != nil {
		return errors.Wrapf(err, "new reader %s", path)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Wrapf(err, "read-all: %s", path)
	}

	if _, err := toml.Decode(string(b), &data); err != nil {
		return errors.Wrapf(err, "decode: %s", string(b))
	}

	c.Workflows[f] = data
	return nil
}

// Close the cache
func (c *Cache) Close() error {
	close(c.done)
	return nil
}
