package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/pcelvng/task"
)

func key(t task.Task) string {
	return t.Type + ":" + t.Info + ":" + t.Created
}

func newStat(c *nsq.Consumer) *stat {
	return &stat{
		inProgress: make(map[string]task.Task),
		success:    &durStats{},
		error:      &durStats{},
		consumer:   c,
	}
}

type stat struct {
	mu         sync.Mutex
	inProgress map[string]task.Task
	success    *durStats
	error      *durStats
	consumer   *nsq.Consumer
}

// NewTask adds a new inProgress task to the queue
func (s *stat) NewTask(t task.Task) {
	s.mu.Lock()
	s.inProgress[key(t)] = t
	s.mu.Unlock()
}

func (s *stat) HandleMessage(msg *nsq.Message) error {
	t, err := task.NewFromBytes(msg.Body)
	if err != nil {
		log.Println("invalid task", err)
		return nil
	}
	s.NewTask(*t)
	return nil
}

// DoneTask adds a completed task to the queue, removes the matching inProgress task
// and calculates the details on the job
func (s *stat) DoneTask(t task.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.inProgress[key(t)]; !found {
		return
	}
	delete(s.inProgress, key(t))
	start, _ := time.Parse(time.RFC3339, t.Started)
	end, _ := time.Parse(time.RFC3339, t.Ended)
	d := end.Sub(start)
	if t.Result == task.ErrResult {
		s.error.Add(d)
	} else if t.Result == task.CompleteResult {
		s.success.Add(d)
	}
}

// Details gives the gather details on the topic being watched
func (s *stat) Details() string {
	successRate := float64(s.success.count) / float64(s.success.count+s.error.count)
	return fmt.Sprintf("Success: %0.2f%% %v\nFailed: %0.2f%% %v\nInProgress: %d\n", successRate, s.success.String(),
		(1.0 - successRate), s.error.String(),
		len(s.inProgress))
}

const precision = 10 * time.Millisecond

type durStats struct {
	Min   time.Duration
	Max   time.Duration
	sum   int64
	count int64
}

func (s *durStats) Add(d time.Duration) {
	if s.count == 0 {
		s.Min = d
		s.Max = d
	}

	if d > s.Max {
		s.Max = d
	} else if d < s.Min {
		s.Min = d
	}
	// truncate times to milliseconds to preserve space
	s.sum += int64(d / precision)
	s.count++
}

func (s *durStats) Average() time.Duration {
	if s.count == 0 {
		return 0
	}
	return time.Duration(s.sum/s.count) * precision
}

func (s durStats) String() string {
	if s.count == 0 {
		return ""
	}
	return fmt.Sprintf("\t%d  min: %v max %v avg:%v", s.count, s.Min, s.Max, s.Average())
}
