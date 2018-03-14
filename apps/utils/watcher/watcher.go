package main

import (
	"log"
	"sync"
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

// watcher is the application runtime object for each rule
// this will watch for files and apply the config rules.
type watcher struct {
	producer bus.Producer

	stop      chan struct{}
	appOpt    *options
	rule      *Rule
	files     *FileList // stats files are the current files in the directory
	cache     *FileList // each watcher has it's own cache to check against for changes
	lookback  int       // the number of hours to look back in previous folders based on date
	frequency string    // the duration between checking for new files
}

// FileList will save file stats for the files and cache that are found in each watch rule
type FileList struct {
	filesMap map[string]*stat.Stats // unique list of full file paths

	mu sync.RWMutex
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
			files:     NewFilesList(),
			cache:     NewFilesList(),
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

	for ; ; time.Sleep(d) {
		// update the files and cache and run the watchers rules
		today := chron.ThisHour()
		nowPath := tmpl.Parse(w.rule.PathTemplate, today.AsTime())
		lookbackPaths := getLookbackPaths(w.rule.PathTemplate, today, w.lookback)

		w.process(nowPath)
		w.process(lookbackPaths...)
	}
}

func (w *watcher) process(path ...string) {
	err := w.addFiles(path...)
	if err != nil {
		log.Println("can not watch:", err)
		return
	}
	w.CreateTasks(CompareFileList(w.cache, w.files))
	w.SetNewCache()
}

// get the unique lookback paths to check for all paths in the lookback time frame
func getLookbackPaths(pathTmpl string, start chron.Hour, lookback int) []string {
	paths := make([]string, 0)
	uniquePaths := make(map[string]interface{})
	for h := 1; h <= lookback; h++ {
		path := tmpl.Parse(pathTmpl, start.AddHours(h*-1).AsTime())
		uniquePaths[path] = nil
	}

	for k, _ := range uniquePaths {
		paths = append(paths, k)
	}
	return paths
}

// addFiles adds the current files from the directory listing
func (w *watcher) addFiles(paths ...string) error {
	for _, p := range paths {
		list, err := file.List(p, &file.Options{
			AWSAccessKey: w.appOpt.AWSAccessKey,
			AWSSecretKey: w.appOpt.AWSSecretKey,
		})
		if err != nil {
			log.Println(err)
			continue
		}

		for i, _ := range list {
			w.files.Add(list[i].Path, &list[i])
		}
	}

	return nil
}

// SetNewCache will reset the watcher cache to the current watcher files
func (w *watcher) SetNewCache() {
	w.cache = NewFilesList()
	for k, v := range w.files.filesMap {
		w.cache.Add(k, v)
	}
}

// CreateTasks uses the watcher producer to send to the current Bus
// using the options topic (default if not set)
func (w *watcher) CreateTasks(fileList *FileList) {
	json := jsoniter.ConfigFastest
	fileList.mu.RLock()
	defer fileList.mu.RUnlock()

	for _, fileStats := range fileList.filesMap {
		b, _ := json.Marshal(fileStats)
		w.producer.Send(w.appOpt.Topic, b)
	}
}

// Create a new FileList struct with a filesMap
func NewFilesList() *FileList {
	return &FileList{
		filesMap: make(map[string]*stat.Stats),
	}
}

// Add will add the file stats to the cached list of files (already accounted for)
func (w *FileList) Add(filePath string, stats *stat.Stats) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.filesMap[filePath] = stats
}

// Remove will delete the file stats from the cached list of files
func (w *FileList) Remove(filePath string, stats *stat.Stats) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.filesMap, filePath)
}

// CompareFileList will check the keys of each of the FileList maps
// if any entries are not listed in the cache a new list will
// be returned with the missing entries
func CompareFileList(cache *FileList, files *FileList) *FileList {
	newList := NewFilesList()
	for k, v := range files.filesMap {
		if _, ok := cache.filesMap[k]; !ok {
			newList.Add(k, v)
		}
	}

	return newList
}
