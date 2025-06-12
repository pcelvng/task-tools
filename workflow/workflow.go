package workflow

import (
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hydronica/toml"
	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/file"
)

type Phase struct {
	Task      string // Should use Topic() and Job() for access
	Rule      string
	DependsOn string // Task that the previous workflow depends on
	Retry     int
	Template  string // template used to create the task
}

func (p Phase) IsEmpty() bool {
	return p.Task == "" && p.Rule == "" && p.DependsOn == "" && p.Template == ""
}

// Job portion of the Task
func (p Phase) Job() string {
	s := strings.Split(p.Task, ":")
	if len(s) > 1 {
		return s[1]
	}

	r, _ := url.ParseQuery(p.Rule)
	if j := r.Get("job"); j != "" {
		return j
	}
	return ""
}

// Topic portion of the Task
func (p Phase) Topic() string {
	s := strings.Split(p.Task, ":")
	return s[0]
}

type Workflow struct {
	Checksum string  // md5 hash for the file to check for changes
	Phases   []Phase `toml:"phase"`
}

type Cache struct {
	done  chan struct{}
	path  string
	isDir bool
	fOpts file.Options
	mutex sync.RWMutex

	Workflows map[string]Workflow // the key is the filename for the workflow
}

// IsDir returns true if the original workflow path is a folder rather than a file
func (c *Cache) IsDir() bool {
	return c.isDir
}

// New returns a Cache used to manage auto updating a workflow
func New(path string, opts *file.Options) (*Cache, error) {
	c := &Cache{
		done:      make(chan struct{}),
		Workflows: make(map[string]Workflow),
		path:      path,
	}
	if opts != nil {
		c.fOpts = *opts
	}
	sts, err := file.Stat(path, opts)
	if err != nil {
		return nil, fmt.Errorf("problem with path %s %w", path, err)
	}
	c.isDir = sts.IsDir
	_, err = c.Refresh()
	return c, err
}

// Search the all workflows within the cache anc return the first
// matching phase with the specific task and job (optional)
func (c *Cache) Search(task, job string) (path string, ph Phase) {
	if c == nil {
		return "", Phase{}
	}
	for key, w := range c.Workflows {
		for _, p := range w.Phases {
			if p.Topic() == task {
				if job == "" {
					return key, p
				}
				if job == p.Job() {
					return key, p
				}
			}
		}
	}
	return "", Phase{}
}

// Get the Phase associated with the task
// looks for matching phases within a workflow defined in meta
// that matches the task Type and job.
func (c *Cache) Get(t task.Task) Phase {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	values, _ := url.ParseQuery(t.Meta)
	key := values.Get("workflow")
	job := t.Job
	if job == "" {
		job = values.Get("job")
	}

	if key == "*" { // search all workflows for first match
		for _, phases := range c.Workflows {
			for _, w := range phases.Phases {
				if w.Topic() == t.Type {
					if job == "" {
						return w
					}
					if w.Job() == job {
						return w
					}
				}
			}
		}
		return Phase{}
	}

	for _, w := range c.Workflows[key].Phases {
		if w.Topic() == t.Type {
			if job == "" {
				return w
			}
			if w.Job() == job {
				return w
			}
		}
	}

	return Phase{}
}

// Children of the given task t, a child phase is one that dependsOn another task
// Empty slice will be returned if no children are found.
// A task without a type or meta data containing the workflow info
// will result in an error
func (c *Cache) Children(t task.Task) []Phase {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if t.Type == "" {
		return nil
	}
	values, _ := url.ParseQuery(t.Meta)
	result := make([]Phase, 0)

	key := values.Get("workflow")
	if t.Job == "" {
		t.Job = values.Get("job")
	}
	if key == "" {
		return nil
	}

	for _, w := range c.Workflows[key].Phases {
		v := strings.Split(w.DependsOn, ":")
		depends := v[0]
		var j string
		if len(v) > 1 {
			j = v[1]
		}

		if depends == t.Type {
			if j == "" || j == t.Job {
				result = append(result, w)
			}
		}
	}
	return result
}

// Refresh checks the cache and reloads any files if the checksum has changed.
func (c *Cache) Refresh() (changedFiles []string, err error) {
	if !c.isDir {
		f, err := c.loadFile(c.path, &c.fOpts)
		if len(f) > 0 {
			changedFiles = append(changedFiles, f)
		}
		return changedFiles, err
	}

	//list and read all files
	allFiles, err := listAllFiles(c.path, &c.fOpts)
	if err != nil {
		return changedFiles, err
	}

	errs := appenderr.New()
	for _, s := range allFiles {
		f, err := c.loadFile(s, &c.fOpts)
		if err != nil {
			errs.Add(err)
		}
		if len(f) > 0 {
			changedFiles = append(changedFiles, f)
		}
	}

	// remove deleted workflows
	for key := range c.Workflows {
		found := false
		for _, v := range allFiles {
			f := c.filePath(v)
			if f == key {
				found = true
				break
			}
		}
		if !found {
			delete(c.Workflows, key)
			changedFiles = append(changedFiles, "-"+key)
		}
	}

	return changedFiles, errs.ErrOrNil()
}

// listAllFiles recursively lists all files in a folder and sub-folders
func listAllFiles(p string, opts *file.Options) ([]string, error) {
	files := make([]string, 0)
	sts, err := file.List(p, opts)
	if err != nil {
		return nil, err
	}
	for _, f := range sts {
		if f.IsDir {
			s, err := listAllFiles(f.Path, opts)
			if err != nil {
				return nil, err
			}
			files = append(files, s...)
			continue
		}
		files = append(files, f.Path)
	}
	return files, nil
}

// loadFile checks a files checksum and updates map if required
// loaded file name is returned
func (c *Cache) loadFile(path string, opts *file.Options) (f string, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	f = c.filePath(path)
	sts, err := file.Stat(path, opts)
	data := c.Workflows[f]
	// permission issues
	if err != nil {
		return "", fmt.Errorf("stats %s %w", path, err)
	}
	// We can't process a directory here
	if sts.IsDir {
		return "", fmt.Errorf("can not read directory %s", path)
	}
	// check if file has changed
	if data.Checksum == sts.Checksum {
		return "", nil
	}
	data.Checksum = sts.Checksum

	r, err := file.NewReader(path, opts)
	if err != nil {
		return "", fmt.Errorf("new reader %s %w", path, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("read-all: %s %w", path, err)
	}
	d := Workflow{
		Checksum: data.Checksum,
	}
	if _, err := toml.Decode(string(b), &d); err != nil {
		return "", fmt.Errorf("decode: %s %w", string(b), err)
	}

	c.Workflows[f] = d

	return f, nil
}

// filePath returns a filePath consist of all unique part
// after the path set in the cache
func (c *Cache) filePath(p string) (s string) {
	path := strings.TrimLeft(c.path, ".")
	if i := strings.LastIndex(p, path); i != -1 {
		s = strings.TrimLeft(p[i+len(path):], "/")
	}
	if s == "" {
		_, s = filepath.Split(p)
	}
	return s
}

// Close the cache
func (c *Cache) Close() error {
	close(c.done)
	return nil
}
