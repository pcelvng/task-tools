package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"regexp"

	"github.com/jbsmith7741/go-tools/uri"
	"github.com/pcelvng/task"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
)

var (
	files   = flag.Bool("files", false, "parse task files stats")
	path    = flag.Bool("path", false, "include file path in breakdown")
	version = flag.Bool("v", false, "show version")
)

type doneStats struct {
	CompletedCount int
	CompletedTimes []time.Time

	ErrorCount int
	ErrorTimes []time.Time
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

func main() {
	scanner := bufio.NewScanner(bufio.NewReader(os.Stdin))
	flag.Parse()
	if *version {
		fmt.Println(tools.String())
		os.Exit(0)
	}

	var result []string
	if *files {
		result = filesTopic(scanner)
	} else {
		result = doneTopic(scanner)
	}

	sort.Strings(result)
	for _, s := range result {
		fmt.Println(s)
	}

}

func doneTopic(scanner *bufio.Scanner) []string {
	data := make(map[string]*doneStats)
	for scanner.Scan() {
		var tsk task.Task
		json.Unmarshal(scanner.Bytes(), &tsk)
		var t getTime
		uri.Unmarshal(tsk.Info, &t)
		var tm time.Time
		if !t.Hour.IsZero() {
			tm = t.Hour
		} else if !t.Day.IsZero() {
			tm = t.Day
		} else if !time.Time(t.Date).IsZero() {
			tm = time.Time(t.Date)
		} else if !(time.Time)(t.PathTime).IsZero() {
			tm = time.Time(t.PathTime)
		}

		stats := &doneStats{
			CompletedTimes: make([]time.Time, 0),
			ErrorTimes:     make([]time.Time, 0),
		}
		topic := tsk.Type
		if *path {
			topic += "\t" + rootPath(tsk.Info, tm)
		}
		if v, found := data[topic]; !found {
			data[topic] = stats
		} else {
			stats = v
		}

		if tsk.Result == task.ErrResult {
			stats.ErrorCount++
			stats.ErrorTimes = append(stats.ErrorTimes, tm)
		} else {
			stats.CompletedCount++
			stats.CompletedTimes = append(stats.CompletedTimes, tm)
		}

	}
	if scanner.Err() != nil {
		fmt.Println(scanner.Err())
	}
	lines := make([]string, 0, len(data))
	for name, v := range data {
		ln := fmt.Sprintf("%s:\n\tComplete %3d  %v", name, v.CompletedCount, printDates(v.CompletedTimes))
		if v.ErrorCount > 0 {
			ln += fmt.Sprintf("\n\tError    %3d  %v", v.ErrorCount, printDates(v.ErrorTimes))
		}
		lines = append(lines, ln)
	}
	return lines
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

func filesTopic(scanner *bufio.Scanner) []string {
	data := make(map[string][]time.Time)
	for scanner.Scan() {
		var sts stat.Stats
		json.Unmarshal(scanner.Bytes(), &sts)
		tm := tmpl.PathTime(sts.Path)
		root := rootPath(sts.Path, tm)
		if v, found := data[root]; found {
			data[root] = append(v, tm)
		} else {
			data[root] = []time.Time{tm}
		}

	}
	if scanner.Err() != nil {
		fmt.Println(scanner.Err())
	}
	lines := make([]string, 0, len(data))
	for name, v := range data {
		lines = append(lines, fmt.Sprintf("%s:\n%d\t\t%v", name, len(v), printDates(v)))
	}
	return lines
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
