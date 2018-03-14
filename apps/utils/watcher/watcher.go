package main

import (
	"log"
	"time"

	"github.com/dustinevan/chron"
	"github.com/json-iterator/go"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task/bus"
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
	producer, err := bus.NewProducer(appOpt.Options)
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
func (w *watcher) Close() error {
	// close the producer
	if err := w.producer.Stop(); err != nil {
		return err
	}

	return nil
}

// closeWatchers closes all the current watchers (rules)
func closeWatchers(list []*watcher) error {
	for i, _ := range list {
		err := list[i].Close()
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
		today := chron.ThisHour()
		nowPath := tmpl.Parse(w.rule.PathTemplate, today.AsTime())
		lookbackPaths := getLookbackPaths(w.rule.PathTemplate, today, w.lookback)

		nowFiles := w.process(cache, nowPath)
		lookbackFiles := w.process(cache, lookbackPaths...)

		// set the cache to the current file listings
		cache = buildNewCache(nowFiles, lookbackFiles)
	}
}

// get the current files for the request path(s)
// compare those files with the current cache for this watcher
// find any new files not listed in the cache and send to the Bus
// for each of the new files
func (w *watcher) process(currentCache fileList, path ...string) (currentFiles fileList) {
	currentFiles, err := w.currentFiles(path...)
	if err != nil {
		log.Println("can not watch:", err)
		return
	}
	newTasks := CompareFileList(currentCache, currentFiles)
	w.SendFiles(newTasks)

	return currentFiles
}

// get the unique lookback paths to check for all paths in the lookback time frame
func getLookbackPaths(pathTmpl string, start chron.Hour, lookback int) []string {
	paths := make([]string, 0)
	uniquePaths := make(map[string]interface{})
	// iterate over each hour setting up the path for that hour
	// this is where you could get duplicates if there isn't an hour or day granularity
	for h := 1; h <= lookback; h++ {
		// each hour is back in time, so h * -1 hours backward
		path := tmpl.Parse(pathTmpl, start.AddHours(h*-1).AsTime())
		uniquePaths[path] = nil
	}

	for k, _ := range uniquePaths {
		paths = append(paths, k)
	}
	return paths
}

// currentFiles retrieves the current files from the directory path(s)
func (w watcher) currentFiles(paths ...string) (fileList, error) {
	fileList := make(map[string]*stat.Stats)
	for _, p := range paths {
		list, err := file.List(p, &file.Options{
			AWSAccessKey: w.appOpt.AWSAccessKey,
			AWSSecretKey: w.appOpt.AWSSecretKey,
		})
		if err != nil {
			log.Println(err)
			continue
		}
		// iterate over the list to setup the new complete fileList
		for i, _ := range list {
			fileList[list[i].Path] = &list[i]
		}
	}

	return fileList, nil
}

// buildNewCache takes all the file lists and combines them into one new cache
func buildNewCache(fileLists ...fileList) (newCache fileList) {
	newCache = make(fileList)
	for _, list := range fileLists {
		for f, s := range list {
			newCache[f] = s
		}
	}
	return newCache
}

// SendFiles uses the watcher producer to send to the current Bus
// using the options topic (default if not set)
func (w *watcher) SendFiles(files fileList) {
	json := jsoniter.ConfigFastest

	for _, f := range files {
		b, _ := json.Marshal(f)
		w.producer.Send(w.appOpt.Topic, b)
	}
}

// CompareFileList will check the keys of each of the FileList maps
// if any entries are not listed in the cache a new list will
// be returned with the missing entries
func CompareFileList(cache, current fileList) (newFiles fileList) {
	newFiles = make(fileList)
	for k, v := range current {
		if _, found := cache[k]; !found {
			newFiles[k] = v
		}
	}

	return newFiles
}
