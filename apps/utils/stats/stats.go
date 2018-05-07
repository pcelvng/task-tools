package main

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jbsmith7741/go-tools/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/tmpl"
)

const (
	percision = 10 * time.Millisecond
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
	s.sum += int64(d / percision)
	s.count++
}

func (s *durStats) Average() time.Duration {
	return time.Duration(s.sum/s.count) * percision
}

func (s durStats) String() string {
	return fmt.Sprintf("\tmin: %v max %v avg:%v", s.Min, s.Max, s.Average())
}

func (stats *taskStats) Add(tsk task.Task) {
	tm := taskTime(tsk)
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

func taskTime(tsk task.Task) time.Time {
	var t getTime
	uri.Unmarshal(tsk.Info, &t)

	if !t.Hour.IsZero() {
		return t.Hour
	} else if !t.Day.IsZero() {
		return t.Day
	} else if !time.Time(t.Date).IsZero() {
		return time.Time(t.Date)
	} else if !(time.Time)(t.PathTime).IsZero() {
		return time.Time(t.PathTime)
	}
	return time.Time{}
}

type pathTime time.Time

func (p *pathTime) UnmarshalText(b []byte) error {
	t := tmpl.PathTime(string(b))
	*p = pathTime(t)
	return nil
}

type getTime struct {
	PathTime pathTime  `uri:"path"`
	Day      time.Time `uri:"day"`
	Date     day       `uri:"date"`
	Hour     time.Time `uri:"hour"`
}
type day time.Time

func (d *day) UnmarshalText(b []byte) error {
	t, err := time.Parse("2006-01-02", string(b))
	if err == nil {
		*d = day(t)
	}
	return err
}

var regWord = regexp.MustCompile(`^[A-z]*$`)

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

func printDates(dates []time.Time) string {
	tFormat := "2006/01/02T15"
	if len(dates) == 0 {
		return ""
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })
	prev := dates[0]
	s := prev.Format(tFormat)
	series := false
	for _, t := range dates {
		diff := t.Truncate(time.Hour).Sub(prev.Truncate(time.Hour))
		if diff != time.Hour && diff != 0 {
			if series {
				s += "-" + prev.Format(tFormat)
			}
			s += "," + t.Format(tFormat)
			series = false
		} else if diff == time.Hour {
			series = true
		}
		prev = t
	}
	if series {
		s += "-" + prev.Format(tFormat)
	}

	//check for daily records only
	if !strings.Contains(s, "-") {
		days := strings.Split(s, ",")
		prev, _ := time.Parse(tFormat, days[0])
		dailyString := prev.Format("2006/01/02")
		series = false

		for i := 1; i < len(days); i++ {
			tm, _ := time.Parse(tFormat, days[i])
			if r := tm.Sub(prev) % (24 * time.Hour); r != 0 {
				return s
			}
			if tm.Sub(prev) != 24*time.Hour {
				if series {
					dailyString += "-" + prev.Format("2006/01/02")
					series = false
				}
				dailyString += "," + tm.Format("2006/01/02")

			} else {
				series = true
			}
			prev = tm
		}
		if series {
			return dailyString + "-" + prev.Format("2006/01/02")
		}
		return dailyString
	}
	return s
}
