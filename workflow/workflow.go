package workflow

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pkg/errors"
)

type Phase struct {
	Task      string // doubles as the Name of the topic to send data to
	Rule      string
	DependsOn string // Task that the previous workflow depends on
	Retry     int
	Template  string // template used to create the task
}

func (p Phase) IsEmpty() bool {
	return p.Task == "" && p.Rule == "" && p.DependsOn == "" && p.Template == ""
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
		return nil, errors.Wrapf(err, "problem with path %s", path)
	}
	c.isDir = sts.IsDir
	_, err = c.Refresh()
	return c, err
}

// Parent phase for the specified workflow file.
// A parent phase is one that doesn't depend on any other tasks
func (r Workflow) Parent() (p []Phase) {
	for _, w := range r.Phases {
		if w.DependsOn == "" {
			p = append(p, w)
		}
	}
	return p
}

// Get the Phase associated with the task t
func (c *Cache) Get(t task.Task) Phase {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	values, _ := url.ParseQuery(t.Meta)
	key := values.Get("workflow")
	job := values.Get("job")

	if key == "*" { // search all workflows for first match
		for _, phases := range c.Workflows {
			for _, w := range phases.Phases {
				if w.Task == t.Type {
					if job == "" {
						return w
					}
					v, _ := url.ParseQuery(w.Rule)
					if v.Get("job") == job {
						return w
					}
				}
			}
		}
		return Phase{}
	}

	for _, w := range c.Workflows[key].Phases {
		if w.Task == t.Type {
			if job == "" {
				return w
			}
			v, _ := url.ParseQuery(w.Rule)
			if v.Get("job") == job {
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
	if key == "" {
		return nil
	}
	for _, w := range c.Workflows[key].Phases {
		if w.DependsOn == t.Type {
			result = append(result, w)
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
		return "", errors.Wrapf(err, "stats %s", path)
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
		return "", errors.Wrapf(err, "new reader %s", path)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return "", errors.Wrapf(err, "read-all: %s", path)
	}

	if _, err := toml.Decode(string(b), &data); err != nil {
		return "", errors.Wrapf(err, "decode: %s", string(b))
	}

	c.Workflows[f] = data

	return f, nil
}

// filePath returns a filePath consist of all unique part
// after the path set in the cache
func (c *Cache) filePath(p string) string {
	s := strings.TrimLeft(strings.Replace(p, c.path, "", 1), "/")
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
