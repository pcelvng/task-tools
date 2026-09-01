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

func resultAllowed(result string, filter *TaskFilter) bool {
	if filter == nil || len(filter.Result) == 0 {
		return true
	}
	return sliceContains(filter.Result, result)
}

func (stats *Stats) recordTime(result string, tm time.Time) {
	switch result {
	case ResultError:
		stats.ErrorCount++
		stats.ErrorTimes = append(stats.ErrorTimes, tm)
	case ResultAlert:
		stats.AlertCount++
		stats.AlertTimes = append(stats.AlertTimes, tm)
	case ResultWarn:
		stats.WarnCount++
		stats.WarnTimes = append(stats.WarnTimes, tm)
	case ResultRunning:
		stats.RunningCount++
		stats.RunningTimes = append(stats.RunningTimes, tm)
	default:
		stats.CompletedCount++
		stats.CompletedTimes = append(stats.CompletedTimes, tm)
	}
}

func incrementHourly(result string, hour int, total *TaskCounts, hourly *[24]TaskCounts) {
	switch result {
	case ResultError:
		hourly[hour].Error++
		total.Error++
	case ResultAlert:
		hourly[hour].Alert++
		total.Alert++
	case ResultWarn:
		hourly[hour].Warn++
		total.Warn++
	case ResultRunning:
		hourly[hour].Running++
		total.Running++
	default:
		hourly[hour].Completed++
		total.Completed++
	}
	hourly[hour].Total++
	total.Total++
}

func (stats *Stats) Add(tsk task.Task) {
	tm := tmpl.TaskTime(tsk)
	result := string(tsk.Result)
	stats.recordTime(result, tm)

	if result != ResultComplete {
		return
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

// HourlyCounts returns total and hourly counts with optional filtering by type, job, and result.
// ID filtering is not supported here; use SQLite.GetHourlyCountsByDate when filter.ID is set.
// The hourly array contains 24 TaskCounts where index represents the hour (0-23).
func (ts TaskStats) HourlyCounts(filter *TaskFilter) (TaskCounts, [24]TaskCounts) {
	var total TaskCounts
	var hourly [24]TaskCounts

	for key, stats := range ts {
		// Apply type and job filters
		if filter != nil {
			// Parse key format "type:job"
			parts := strings.SplitN(key, ":", 2)
			taskType := parts[0]
			taskJob := ""
			if len(parts) == 2 {
				taskJob = parts[1]
			}

			if len(filter.Type) > 0 && !sliceContains(filter.Type, taskType) {
				continue
			}

			if len(filter.Job) > 0 && !sliceContains(filter.Job, taskJob) {
				continue
			}
		}

		addTimesToHourly(stats.CompletedTimes, ResultComplete, filter, &total, &hourly)
		addTimesToHourly(stats.ErrorTimes, ResultError, filter, &total, &hourly)
		addTimesToHourly(stats.AlertTimes, ResultAlert, filter, &total, &hourly)
		addTimesToHourly(stats.WarnTimes, ResultWarn, filter, &total, &hourly)
		addTimesToHourly(stats.RunningTimes, ResultRunning, filter, &total, &hourly)
	}

	return total, hourly
}

func addTimesToHourly(times []time.Time, result string, filter *TaskFilter, total *TaskCounts, hourly *[24]TaskCounts) {
	if !resultAllowed(result, filter) {
		return
	}
	for _, t := range times {
		incrementHourly(result, t.Hour(), total, hourly)
	}
}

func addTaskHourlyCounts(tsk task.Task, filter *TaskFilter, total *TaskCounts, hourly *[24]TaskCounts) {
	result := string(tsk.Result)
	if !resultAllowed(result, filter) {
		return
	}
	incrementHourly(result, tmpl.TaskTime(tsk).Hour(), total, hourly)
}
