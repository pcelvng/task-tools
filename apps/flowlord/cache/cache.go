package cache

import (
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

// AlertRecord represents an alert stored in the database
type AlertRecord struct {
	ID          int64     `json:"id"`
	TaskID      string    `json:"task_id"`
	TaskType    string    `json:"task_type"`
	Job         string    `json:"job"`
	Msg         string    `json:"msg"`
	CreatedAt   time.Time `json:"created_at"`
	TaskCreated time.Time `json:"task_created"`
}

// SummaryLine represents a grouped alert summary for dashboard display
type SummaryLine struct {
	Key       string    `json:"key"`        // "task.type:job"
	Count     int       `json:"count"`      // number of alerts
	FirstTime time.Time `json:"first_time"` // first alert time
	LastTime  time.Time `json:"last_time"`  // last alert time
	TimeRange string    `json:"time_range"` // formatted time range
}

type Cache interface {
	Add(task.Task)
	Get(id string) TaskJob
	

	// todo: listener for cache expiry?
}

func NewMemory(ttl time.Duration) *Memory {
	if ttl < time.Hour {
		ttl = time.Hour
	}
	return &Memory{
		ttl:   ttl,
		cache: make(map[string]TaskJob),
	}

}

type Memory struct {
	ttl   time.Duration
	cache map[string]TaskJob
	mu    sync.RWMutex
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
	total := len(c.cache)
	c.mu.Lock()
	for k, v := range c.cache {
		// remove expired items
		if t.Sub(v.LastUpdate) > c.ttl {
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
	if t.ID == "" || c == nil {
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
	if c == nil {
		return data
	}
	c.mu.RLock()
	for _, v := range c.cache {
		for _, t := range v.Events {
			job := t.Job
			if job == "" {
				v, _ := url.ParseQuery(t.Meta)
				job = v.Get("job")
			}
			key := strings.TrimRight(t.Type+":"+job, ":")
			stat, found := data[key]
			if !found {
				stat = &Stats{
					CompletedTimes: make([]time.Time, 0),
					ErrorTimes:     make([]time.Time, 0),
					ExecTimes:      &DurationStats{},
				}
				data[key] = stat
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

// SendFunc extends the given producers send function by adding any task sent to the cache.
func (m *Memory) SendFunc(p bus.Producer) func(string, *task.Task) error {
	return func(topic string, tsk *task.Task) error {
		m.Add(*tsk)
		return p.Send(topic, tsk.JSONBytes())
	}
}

