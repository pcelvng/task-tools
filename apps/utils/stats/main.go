package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
)

var (
	files   = flag.Bool("files", false, "parse task files stats")
	path    = flag.Bool("path", false, "include file path in breakdown")
	version = flag.Bool("v", false, "show version")
)

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
	data := make(map[string]*taskStats)
	for scanner.Scan() {
		var tsk task.Task
		json.Unmarshal(scanner.Bytes(), &tsk)

		stats := &taskStats{
			CompletedTimes: make([]time.Time, 0),
			ErrorTimes:     make([]time.Time, 0),
			ExecTimes:      &durStats{},
		}
		topic := tsk.Type
		if *path {
			topic += "\t" + rootPath(tsk.Info, taskTime(tsk))
		}
		if v, found := data[topic]; !found {
			data[topic] = stats
		} else {
			stats = v
		}
		stats.Add(tsk)
	}
	if scanner.Err() != nil {
		fmt.Println(scanner.Err())
	}
	lines := make([]string, 0, len(data))
	for name, v := range data {
		ln := name + "\n"
		ln += v.ExecTimes.String()
		ln += fmt.Sprintf("\n\tComplete %3d  %v", v.CompletedCount, printDates(v.CompletedTimes))
		if v.ErrorCount > 0 {
			ln += fmt.Sprintf("\n\tError    %3d  %v", v.ErrorCount, printDates(v.ErrorTimes))
		}
		lines = append(lines, ln)
	}
	return lines
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
