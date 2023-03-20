package cache

import (
	"fmt"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/tmpl"
)

const (
	precision = 10 * time.Millisecond
)

type Stats struct {
	CompletedCount int
	CompletedTimes []time.Time

	ErrorCount int
	ErrorTimes []time.Time

	ExecTimes *DurationStats
}

type DurationStats struct {
	Min   time.Duration
	Max   time.Duration
	sum   int64
	count int64
}

func (s *DurationStats) Add(d time.Duration) {
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

func (s *DurationStats) Average() time.Duration {
	if s.count == 0 {
		return 0
	}
	return time.Duration(s.sum/s.count) * precision
}

func (s DurationStats) String() string {
	if s.count == 0 {
		return ""
	}
	return fmt.Sprintf("\tmin: %v max %v avg:%v", s.Min, s.Max, s.Average())
}

func (stats *Stats) Add(tsk task.Task) {
	tm := tmpl.InfoTime(tsk.Info)
	if tsk.Result == task.ErrResult {
		stats.ErrorCount++
		stats.ErrorTimes = append(stats.ErrorTimes, tm)
		return
	}

	stats.CompletedCount++
	stats.CompletedTimes = append(stats.CompletedTimes, tm)

	end, _ := time.Parse(time.RFC3339, tsk.Ended)
	start, _ := time.Parse(time.RFC3339, tsk.Started)
	stats.ExecTimes.Add(end.Sub(start))
}

type pathTime time.Time

func (p *pathTime) UnmarshalText(b []byte) error {
	t := tmpl.PathTime(string(b))
	*p = pathTime(t)
	return nil
}
