package sqlite

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	gtools "github.com/jbsmith7741/go-tools"
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

	AlertCount int
	AlertTimes []time.Time

	WarnCount int
	WarnTimes []time.Time

	RunningCount int
	RunningTimes []time.Time

	ExecTimes *DurationStats
}

func (s *Stats) MarshalJSON() ([]byte, error) {
	type count struct {
		Count int
		Times string
	}

	v := struct {
		Min      string `json:"min"`
		Max      string `json:"max"`
		Average  string `json:"avg"`
		Complete count  `json:"complete"`
		Error    count  `json:"error"`
	}{
		Min:     gtools.PrintDuration(s.ExecTimes.Min),
		Max:     gtools.PrintDuration(s.ExecTimes.Max),
		Average: gtools.PrintDuration(s.ExecTimes.Average()),
		Complete: count{
			Count: s.CompletedCount,
			Times: tmpl.PrintDates(s.CompletedTimes),
		},
		Error: count{
			Count: s.ErrorCount,
			Times: tmpl.PrintDates(s.ErrorTimes),
		},
	}
	return json.Marshal(v)
}

func (s Stats) String() string {
	r := s.ExecTimes.String()
	if s.CompletedCount > 0 {
		r += fmt.Sprintf("\n\tComplete: %d %v", s.CompletedCount, tmpl.PrintDates(s.CompletedTimes))
	}
	if s.ErrorCount > 0 {
		r += fmt.Sprintf("\n\tError: %d %v", s.ErrorCount, tmpl.PrintDates(s.ErrorTimes))
	}

	return r + "\n"
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

func (s *DurationStats) String() string {
	return fmt.Sprintf("min: %v max: %v avg: %v",
		s.Min, s.Max, s.Average())
}

func (stats *Stats) Add(tsk task.Task) {
	tm := tmpl.TaskTime(tsk)
	
	// Handle different result types
	switch tsk.Result {
	case task.ErrResult:
		stats.ErrorCount++
		stats.ErrorTimes = append(stats.ErrorTimes, tm)
		return
	case "alert":
		stats.AlertCount++
		stats.AlertTimes = append(stats.AlertTimes, tm)
		return
	case "warn":
		stats.WarnCount++
		stats.WarnTimes = append(stats.WarnTimes, tm)
		return
	case "":
		// Empty result means task is running
		stats.RunningCount++
		stats.RunningTimes = append(stats.RunningTimes, tm)
		return
	default:
		// Assume "complete" or any other result is a completion
		stats.CompletedCount++
		stats.CompletedTimes = append(stats.CompletedTimes, tm)
	}

	// Track execution time for completed tasks
	if tsk.Ended != "" && tsk.Started != "" {
		end, _ := time.Parse(time.RFC3339, tsk.Ended)
		start, _ := time.Parse(time.RFC3339, tsk.Started)
		stats.ExecTimes.Add(end.Sub(start))
	}
}

type pathTime time.Time

func (p *pathTime) UnmarshalText(b []byte) error {
	t := tmpl.PathTime(string(b))
	*p = pathTime(t)
	return nil
}

// TaskCounts represents aggregate counts of tasks by result status
type TaskCounts struct {
	Total     int
	Completed int
	Error     int
	Alert     int
	Warn      int
	Running   int
}

// TaskStats is a map of task keys (type:job) to their statistics
type TaskStats map[string]*Stats

// UniqueTypes returns a sorted list of unique task types
func (ts TaskStats) UniqueTypes() []string {
	typeSet := make(map[string]struct{})
	for key := range ts {
		// Split the key to get type (everything before the first colon)
		if idx := strings.Index(key, ":"); idx > 0 {
			typeSet[key[:idx]] = struct{}{}
		} else {
			// No colon means the entire key is the type
			typeSet[key] = struct{}{}
		}
	}
	
	types := make([]string, 0, len(typeSet))
	for t := range typeSet {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// JobsByType returns jobs organized by type
func (ts TaskStats) JobsByType() map[string][]string {
	jobsByType := make(map[string][]string)
	
	for key := range ts {
		// Split key into type and job
		parts := strings.SplitN(key, ":", 2)
		if len(parts) == 2 {
			typ := parts[0]
			job := parts[1]
			if job != "" {
				jobsByType[typ] = append(jobsByType[typ], job)
			}
		}
	}
	
	// Sort jobs for each type
	for typ := range jobsByType {
		sort.Strings(jobsByType[typ])
	}
	
	return jobsByType
}

// TotalCounts returns aggregate result counts across all tasks
func (ts TaskStats) TotalCounts() TaskCounts {
	var counts TaskCounts
	
	for _, stats := range ts {
		counts.Total += stats.CompletedCount + stats.ErrorCount + stats.AlertCount + stats.WarnCount + stats.RunningCount
		counts.Completed += stats.CompletedCount
		counts.Error += stats.ErrorCount
		counts.Alert += stats.AlertCount
		counts.Warn += stats.WarnCount
		counts.Running += stats.RunningCount
	}
	
	return counts
}
