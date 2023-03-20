package cache

import (
	"sync"
	"time"

	"github.com/pcelvng/task"
)

type Cache interface {
	Add(task.Task)
	Get(id string) TaskJob

	// todo: listener for cache expiry?
}

func NewMemory(ttl_minutes int) *Memory {
	return &Memory{
		ttl_Minute: ttl_minutes,
		cache:      make(map[string]TaskJob),
	}

}

type Memory struct {
	ttl_Minute int // time-to-live in minutes?
	cache      map[string]TaskJob
	mu         sync.RWMutex
}

// todo: name to describe info about completed tasks that are within the cache
type TaskJob struct {
	LastUpdate time.Time // time since the last event with id
	Completed  bool
	count      int
	Events     []task.Task
}

type Stat struct {
	Count       int
	Removed     int
	ProcessTime time.Duration
	Unfinished  []task.Task
}

// Recycle iterates through the cache
// clearing all tasks that have been completed within the cache window
// it returns a list of tasks that have not been completed but have expired
func (c *Memory) Recycle() Stat {
	tasks := make([]task.Task, 0)
	t := time.Now()
	ttl := time.Duration(c.ttl_Minute) * time.Minute
	total := len(c.cache)
	c.mu.Lock()
	for k, v := range c.cache {
		// remove expired items
		if t.Sub(v.LastUpdate) > ttl {
			if !v.Completed {
				tasks = append(tasks, v.Events[len(v.Events)-1])
			}
			delete(c.cache, k)
		}
	}
	c.mu.Unlock()
	return Stat{
		Count:       len(c.cache),
		Removed:     total - len(c.cache),
		ProcessTime: time.Since(t),
		Unfinished:  tasks,
	}

}

// Add a task to the cache
// the task must have an id to be added.
func (c *Memory) Add(t task.Task) {
	if t.ID == "" {
		return
	}
	c.mu.Lock()
	job := c.cache[t.ID]
	job.Events = append(job.Events, t)
	if t.Result != "" {
		job.Completed = true
		t, _ := time.Parse(time.RFC3339, t.Ended)
		job.LastUpdate = t
	} else {
		job.Completed = false
		t, _ := time.Parse(time.RFC3339, t.Created)
		job.LastUpdate = t
	}

	c.cache[t.ID] = job
	c.mu.Unlock()
}

func (c *Memory) Recap() map[string]*Stats {
	data := map[string]*Stats{}
	c.mu.RLock()
	for _, v := range c.cache {
		for _, t := range v.Events {
			stat, found := data[t.Type+":"+t.Job]
			if !found {
				stat = &Stats{
					CompletedTimes: make([]time.Time, 0),
					ErrorTimes:     make([]time.Time, 0),
					ExecTimes:      &DurationStats{},
				}
			}
			stat.Add(t)
		}
	}
	c.mu.RUnlock()
	return data
}

// Get the TaskJob info with the given id.
// If the id isn't found a _ is returned
func (c *Memory) Get(id string) TaskJob {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache[id]
}
