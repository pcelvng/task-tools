package main

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/tmpl"
)

const (
	precision = 10 * time.Millisecond
)

type taskStats struct {
	CompletedCount int
	CompletedTimes []time.Time

	ErrorCount int
	ErrorTimes []time.Time

	ExecTimes *durStats
}

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
	return fmt.Sprintf("\tmin: %v max %v avg:%v", s.Min, s.Max, s.Average())
}

func (stats *taskStats) Add(tsk task.Task) {
	tm := tmpl.TaskTime(tsk)
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

var regWord = regexp.MustCompile(`^[A-z]*$`)

// rootPath returns the file path without any timestamp data.
// this can be used to find unique base paths for data organized by date.
func rootPath(path string, tm time.Time) string {
	dir, file := filepath.Split(path)
	slugFound := false
	if i := strings.Index(path, tm.Format("2006/01/02/15")); i != -1 {
		slugFound = true
		path = path[:i]
	} else if i = strings.Index(path, tm.Format("2006/01/02")); i != -1 {
		slugFound = true
		path = path[:i]
	} else if i = strings.Index(path, tm.Format("2006/01")); i != -1 {
		slugFound = true
		path = path[:i]
	} else {
		path = dir
	}

	s := strings.Split(file, ".")[0]
	if regWord.MatchString(s) {
		if slugFound {
			path += "*/"
		}
		path += s
	}
	return path
}
