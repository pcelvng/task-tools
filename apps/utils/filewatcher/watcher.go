package main

import (
	"log"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task/bus"
	chron "gopkg.in/dustinevan/chron.v0"
)

var (
	defaultLookback  = 24
	defaultFrequency = "1h"
)

type fileList map[string]*stat.Stats

// watcher is the application runtime object for each rule
// this will watch for files and apply the config rules.
type watcher struct {
	producer bus.Producer

	stop      chan struct{}
	appOpt    *options
	rule      *Rule
	lookback  int    // the number of hours to look back in previous folders based on date
	frequency string // the duration between checking for new files
}

// newWatchers creates new watchers based on the options provided in configuration files
// there will be a watcher for each rule provided
func newWatchers(appOpt *options) (watchers []*watcher, err error) {
	// producer
	producer, err := bus.NewProducer(appOpt.Bus)
	if err != nil {
		return nil, err
	}

	for _, r := range appOpt.Rules {
		if r.HourLookback == 0 {
			r.HourLookback = defaultLookback
		}

		if r.Frequency == "" {
			r.Frequency = defaultFrequency
		}

		watchers = append(watchers, &watcher{
			producer:  producer,
			appOpt:    appOpt,
			rule:      r,
			lookback:  r.HourLookback,
			frequency: r.Frequency,
		})
	}
	return watchers, err
}

// Close closes the producer and sends sends a close signal
func (w *watcher) close() error {
	// close the producer
	if err := w.producer.Stop(); err != nil {
		return err
	}

	return nil
}

// closeWatchers closes all the current watchers (rules)
func closeWatchers(list []*watcher) error {
	for i := range list {
		err := list[i].close()
		if err != nil {
			return err
		}
	}
	return nil
}

// runWatch starts the loop to continue watching the rule path_template for new files
func (w *watcher) runWatch() (err error) {
	// check for valid duration for the frequency
	d, err := time.ParseDuration(w.frequency)
	if err != nil {
		log.Println("bad frequency", w.rule.Frequency, err)
		return err
	}

	// new cached file list for the current watcher
	cache := make(fileList)

	for ; ; time.Sleep(d) {
		// update the files and cache and run the watchers rules
		currentHour := chron.ThisHour()
		lookbackFiles := getPaths(w.rule.PathTemplate, currentHour, w.lookback)

		// send the new files, re cache those new files
		cache = w.process(cache, lookbackFiles...)
	}
}

// get the current files for the request path(s)
// compare those files with the current cache for this watcher
// find any new files not listed in the cache and send to the Bus
func (w *watcher) process(currentCache fileList, path ...string) (currentFiles fileList) {
	currentFiles = w.currentFiles(path...)
	newFiles := compareFileList(currentCache, currentFiles)
	w.sendFiles(newFiles)
	log.Printf("%v found %d files with %d new files", w.rule.PathTemplate, len(currentFiles), len(newFiles))
	return currentFiles
}

// get the unique paths, check for all paths for each of the lookback hours
func getPaths(pathTmpl string, start chron.Hour, lookback int) []string {
	paths := make([]string, 0)
	uniquePaths := make(map[string]interface{})
	// iterate over each hour setting up the path for that hour
	// this is where you could get duplicates if there isn't an hour or day granularity
	for h := 0; h <= lookback; h++ {
		// each hour is back in time, so h * -1 hours backward
		hourCheck := start.AddHours(h * -1).AsTime()
		path := tmpl.Parse(pathTmpl, hourCheck)
		uniquePaths[path] = nil
	}

	for k := range uniquePaths {
		paths = append(paths, k)
	}
	return paths
}

// currentFiles retrieves the current files from the directory path(s)
func (w watcher) currentFiles(paths ...string) fileList {
	fileList := make(fileList)
	for _, p := range paths {
		list, err := file.List(p, &file.Options{
			AccessKey: w.appOpt.AccessKey,
			SecretKey: w.appOpt.SecretKey,
		})
		if err != nil {
			log.Println(err)
			continue
		}
		// iterate over the list to setup the new complete fileList
		for i := range list {
			if list[i].IsDir {
				continue
			}
			fileList[list[i].Path] = &list[i]
		}
	}

	return fileList
}

// SendFiles uses the watcher producer to send to the current Bus
// using the options topic (default if not set)
func (w *watcher) sendFiles(files fileList) {
	json := jsoniter.ConfigFastest

	for _, f := range files {
		if w.appOpt.FilesTopic != "-" {
			b, _ := json.Marshal(f)
			w.producer.Send(w.appOpt.FilesTopic, b)
		}

		if w.appOpt.TaskTopic != "" {
			t := tmpl.PathTime(f.Path)
			info := tmpl.Parse(w.rule.TaskTemplate, t)
			info = strings.Replace(info, "{WATCH_FILE}", f.Path, -1)

			tsk := task.New(w.appOpt.TaskTopic, info)
			meta := task.NewMeta()
			meta.SetMeta("job", "filewatcher")
			tsk.Meta = meta.GetMeta().Encode()
			b, _ := json.Marshal(tsk)
			w.producer.Send(w.appOpt.TaskTopic, b)
		}

	}
}

// CompareFileList will check the keys of each of the FileList maps
// if any entries are not listed in the cache a new list will
// be returned with the missing or changed entries
func compareFileList(cache, new fileList) (newFiles fileList) {
	newFiles = make(fileList)
	for n, v := range new {
		// if the file was not found in the cache, and the size is not zero
		// add it to the list of new files
		c, found := cache[n]
		if !found && (v.Size > 0 || len(v.Created) > 0) {
			newFiles[n] = v
		}
		// if the file was found in the cache, but the created date is different
		// or the size is different add it to the list of new files
		if found && (c.Created != v.Created || c.Size != v.Size) {
			newFiles[n] = v
		}
	}
	return newFiles
}
