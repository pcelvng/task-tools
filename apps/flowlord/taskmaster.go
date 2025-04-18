package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	gtools "github.com/jbsmith7741/go-tools"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/robfig/cron/v3"

	"github.com/pcelvng/task-tools/apps/flowlord/cache"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/slack"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
)

type taskMaster struct {
	//	options

	initTime      time.Time
	nextUpdate    time.Time
	lastUpdate    time.Time
	path          string
	dur           time.Duration
	producer      bus.Producer
	doneConsumer  bus.Consumer
	filesConsumer bus.Consumer
	fOpts         *file.Options
	doneTopic     string
	failedTopic   string
	taskCache     *cache.SQLite
	*workflow.Cache
	port  int
	cron  *cron.Cron
	slack *Notification
	files []fileRule

	alerts chan task.Task
}

type Notification struct {
	slack.Slack
	ReportPath   string
	MinFrequency time.Duration
	MaxFrequency time.Duration

	file *file.Options
}

type stats struct {
	AppName    string `json:"app_name"`
	Version    string `json:"version"`
	RunTime    string `json:"runtime"`
	NextUpdate string `json:"next_cache"`
	LastUpdate string `json:"last_cache"`

	Workflow map[string]map[string]cEntry `json:"workflow"`
}

type cEntry struct {
	Next     *time.Time `json:"Next,omitempty"`
	Prev     *time.Time `json:"Prev,omitempty"`
	Warning  string     `json:"warning,omitempty"`
	Schedule []string   `json:"Schedule,omitempty"`
	Child    []string   `json:"Child,omitempty"`
}

func New(opts *options) *taskMaster {

	opts.Bus.InTopic = opts.DoneTopic
	if opts.Bus.Bus == "pubsub" {
		opts.Bus.InChannel = opts.DoneTopic + "-flowlord"
	}
	consumer, err := bus.NewConsumer(&opts.Bus)
	if err != nil {
		log.Fatal("done Consumer init", err)
	}
	producer, err := bus.NewProducer(&opts.Bus)
	if err != nil {
		log.Fatal("producer init", err)
	}
	if opts.Slack.MinFrequency == 0 {
		opts.Slack.MinFrequency = 5 * time.Minute
	}
	if opts.Slack.MaxFrequency <= opts.Slack.MinFrequency {
		opts.Slack.MaxFrequency = 16 * opts.Slack.MinFrequency
	}
	db, err := cache.NewSQLite(opts.TaskTTL, "./tasks.db")
	if err != nil {
		log.Fatal("db init", err)
	}

	opts.Slack.file = opts.File
	tm := &taskMaster{
		initTime:     time.Now(),
		taskCache:    db,
		path:         opts.Workflow,
		doneTopic:    opts.DoneTopic,
		failedTopic:  opts.FailedTopic,
		fOpts:        opts.File,
		producer:     producer,
		doneConsumer: consumer,
		port:         opts.Port,
		cron:         cron.New(cron.WithSeconds()),
		dur:          opts.Refresh,
		slack:        opts.Slack,
		alerts:       make(chan task.Task, 20),
	}
	if opts.FileTopic != "" {
		opts.Bus.InTopic = opts.FileTopic
		opts.Bus.InChannel = opts.FileTopic + "-flowlord"
		tm.filesConsumer, err = bus.NewConsumer(&opts.Bus)
		if err != nil {
			log.Println("files consumer: ", err)
			tm.filesConsumer = nil
		}
	}
	return tm
}

// pName creates the phase task name
// topic:job
// topic
func pName(topic, job string) string {
	if job == "" {
		return topic
	}
	return topic + ":" + job
}

func (tm *taskMaster) getAllChildren(topic, workflow, job string) (s []string) {
	for _, c := range tm.Children(task.Task{Type: topic, Meta: "workflow=" + workflow + "&job=" + job}) {
		job := strings.Trim(c.Topic()+":"+c.Job(), ":")
		if children := tm.getAllChildren(c.Task, workflow, c.Job()); len(children) > 0 {
			job += " ➞ " + strings.Join(children, " ➞ ")
		}
		s = append(s, job)
	}
	return s
}

func (tm *taskMaster) refreshCache() ([]string, error) {
	stat := tm.taskCache.Recycle()
	if stat.Removed > 0 {
		log.Printf("task-cache: size %d removed %d time: %v", stat.Count, stat.Removed, stat.ProcessTime)
		for _, t := range stat.Unfinished {
			// add unfinished tasks to alerts channel
			t.Msg += "unfinished task detected"
			tm.alerts <- t
		}
	}

	files, err := tm.Cache.Refresh()
	if err != nil {
		return nil, fmt.Errorf("error reloading workflow: %w", err)
	}
	// if there are values in files, there are changes that need to be reloaded
	if len(files) > 0 {
		log.Println("reloading workflow changes")
		tcron := tm.cron
		tm.cron = cron.New(cron.WithSeconds())
		if err := tm.schedule(); err != nil {
			tm.cron = tcron // revert to old cron schedule
			return files, fmt.Errorf("cron schedule: %w", err)
		} else {
			tcron.Stop()
		}
	}
	tm.lastUpdate = time.Now()
	tm.nextUpdate = tm.lastUpdate.Add(tm.dur)
	return files, nil
}

func (tm *taskMaster) Run(ctx context.Context) (err error) {
	if tm.Cache, err = workflow.New(tm.path, tm.fOpts); err != nil {
		return fmt.Errorf("workflow setup %w", err)
	}

	// refresh the workflow if the file(s) have been changed
	_, err = tm.refreshCache()
	if err != nil {
		log.Fatal(err)
	}
	go func() { // auto refresh cache after set duration
		tick := time.NewTicker(tm.dur)
		for range tick.C {
			if _, err := tm.refreshCache(); err != nil {
				log.Println(err)
			}
		}
	}()

	if err := tm.schedule(); err != nil {
		return fmt.Errorf("cron schedule %w", err)
	}

	go tm.readDone(ctx)
	go tm.readFiles(ctx)

	go tm.StartHandler()
	go tm.slack.handleNotifications(tm.alerts, ctx)
	<-ctx.Done()
	log.Println("shutting down")
	return nil

}

func validatePhase(p workflow.Phase) string {
	if p.DependsOn == "" {
		if p.Rule == "" {
			return "invalid phase: rule and dependsOn are blank"
		}
		// verify at least one valid rule is there
		rules, _ := url.ParseQuery(p.Rule)
		if rules.Get("cron") == "" {
			return fmt.Sprintf("no valid rule found: %v", p.Rule)
		}

		return ""

	}
	// DependsOn != ""
	if p.Rule != "" {
		return fmt.Sprintf("ignored rule: %v", p.Rule)
	}

	return ""
}

// schedule the tasks and refresh the schedule when updated
func (tm *taskMaster) schedule() (err error) {
	errs := make([]error, 0)
	if len(tm.Workflows) == 0 {
		return fmt.Errorf("no workflows found check path %s", tm.path)
	}
	for path, workflow := range tm.Workflows {
		for _, w := range workflow.Phases {
			rules, _ := url.ParseQuery(w.Rule)
			cronSchedule := rules.Get("cron")
			if f := rules.Get("files"); f != "" {
				r := fileRule{
					SrcPattern:   f,
					workflowFile: path,
					Phase:        w,
					CronCheck:    cronSchedule,
				}
				r.CountCheck, _ = strconv.Atoi(rules.Get("count"))

				tm.files = append(tm.files, r)

				//todo: Create a cron job for a task that is cron and files
			}

			if cronSchedule == "" {
				log.Printf("skip: task:%s, rule:%s", w.Task, w.Rule)
				continue
			}

			j, err := tm.NewJob(w, path)
			if err != nil {
				errs = append(errs, fmt.Errorf("issue with %s %w", w.Task, err))
			}

			if _, err = tm.cron.AddJob(cronSchedule, j); err != nil {
				errs = append(errs, fmt.Errorf("invalid rule for %s:%s %s %w", path, w.Task, w.Rule, err))
			}
		}
	}
	tm.cron.Start()
	return errors.Join(errs...)
}

// Process the given task
// 1. check if the task needs to be retried
// 2. start any downstream tasks
// Send retry failed tasks to tm.failedTopic (only if the phase exists in the workflow)
func (tm *taskMaster) Process(t *task.Task) error {
	meta, _ := url.ParseQuery(t.Meta)
	tm.taskCache.Add(*t)
	// attempt to retry
	if t.Result == task.ErrResult {
		p := tm.Get(*t)
		rules, _ := url.ParseQuery(p.Rule)

		r := meta.Get("retry")
		i, _ := strconv.Atoi(r)
		// the task should have a workflow phase
		if p.IsEmpty() {
			return fmt.Errorf("phase not found in %q for %v:%v", meta.Get("workflow"), t.Type, t.Job)
		}
		if p.Retry > i {
			delay := time.Second
			if s := rules.Get("retry_delay"); s != "" {
				delay, _ = time.ParseDuration(s)
				delay = delay + jitterPercent(delay, 40)
				meta.Set("delayed", gtools.PrintDuration(delay))
			}
			t = task.NewWithID(t.Type, t.Info, t.ID)
			t.Job = p.Job()
			i++
			meta.Set("retry", strconv.Itoa(i))
			t.Meta = meta.Encode()
			go func() {
				time.Sleep(delay)
				tm.taskCache.Add(*t)
				if err := tm.producer.Send(t.Type, t.JSONBytes()); err != nil {
					log.Println(err)
				}
			}()
			return nil
		} else { // send to the retry failed topic if retries > p.Retry
			meta.Set("retry", "failed")
			meta.Set("retried", strconv.Itoa(p.Retry))
			t.Meta = meta.Encode()
			if tm.failedTopic != "-" && tm.failedTopic != "" {
				tm.taskCache.Add(*t)
				if err := tm.producer.Send(tm.failedTopic, t.JSONBytes()); err != nil {
					return err
				}
			}
			if tm.slack != nil {
				tm.alerts <- *t
			}
		}

		if t.Result == task.AlertResult && tm.slack != nil {
			if tm.slack != nil {
				tm.alerts <- *t
			}
		}

		return nil
	}

	// start off any children tasks
	if t.Result == task.CompleteResult {
		for _, p := range tm.Children(*t) {
			if !isReady(p.Rule, t.Meta) {
				continue
			}
			info, _ := tmpl.Meta(p.Template, meta)

			taskTime := tmpl.TaskTime(*t)

			info = tmpl.Parse(info, taskTime)

			child := task.NewWithID(p.Topic(), info, t.ID)
			child.Job = p.Job()

			child.Meta = "workflow=" + meta.Get("workflow")
			if v := meta.Get("cron"); v != "" {
				child.Meta += "&cron=" + v
			}
			if child.Job != "" {
				child.Meta += "&job=" + child.Job
			}

			tm.taskCache.Add(*child)
			if err := tm.producer.Send(p.Topic(), child.JSONBytes()); err != nil {
				return err
			}
		}
		return nil
	}
	if t.Result == task.WarnResult {
		// do nothing
		return nil
	}
	return fmt.Errorf("unknown result %q %s", t.Result, t.JSONString())
}

var regexMeta = regexp.MustCompile(`{meta:(\w+)}`)

// isReady checks a task rule for any require fields and verifies
// that all fields are included and valid
func isReady(rule, meta string) bool {
	rules, _ := url.ParseQuery(rule)
	met, _ := url.ParseQuery(meta)
	req := strings.Join(rules["require"], ",")
	for _, m := range regexMeta.FindAllStringSubmatch(req, -1) {
		if s := met.Get(m[1]); s == "" {
			return false
		}
	}
	return true
}

func (tm *taskMaster) readDone(ctx context.Context) {
	for {
		b, done, err := tm.doneConsumer.Msg()
		if done || task.IsDone(ctx) {
			log.Println("stopping done Consumer")
			return
		}
		if err != nil {
			log.Println("done Consumer", err)
			return
		}
		t := &task.Task{}
		if err = json.Unmarshal(b, t); err != nil {
			log.Printf("unmarshal error %q: %s", string(b), err)
			continue
		}
		if err := tm.Process(t); err != nil {
			log.Println(err)
		}
	}
}

func (tm *taskMaster) readFiles(ctx context.Context) {
	if tm.filesConsumer == nil {
		log.Println("no files consumer")
		return
	}
	for {
		b, done, err := tm.filesConsumer.Msg()
		if done || task.IsDone(ctx) {
			log.Println("stopping files Consumer")
			return
		}
		if err != nil {
			log.Println("files Consumer", err)
			return
		}
		s := unmarshalStat(b)
		if err := tm.matchFile(s.Clone()); err != nil {
			log.Println("files: ", err)
		}
	}
}

// handleNotifications gathers all 'failed' tasks and
// sends a summary message every X minutes
// It uses an exponential backoff to limit the number of messages
// ie, (min) 5 -> 10 -> 20 -> 40 -> 80 -> 160 (max)
// The backoff is cleared after no failed tasks occur within the window
func (n *Notification) handleNotifications(taskChan chan task.Task, ctx context.Context) {
	sendChan := make(chan struct{})
	tasks := make([]task.Task, 0)
	go func() {
		dur := n.MinFrequency
		for {
			if len(tasks) > 0 {
				sendChan <- struct{}{}
				if dur *= 2; dur > n.MaxFrequency {
					dur = n.MaxFrequency
				}
				log.Println("wait time ", dur)
			} else if dur != n.MinFrequency {
				dur = n.MinFrequency
				log.Println("Reset ", dur)
			}
			time.Sleep(dur)
		}
	}()
	for {
		select {
		case tsk := <-taskChan:
			// if the task result is an alert result, send a slack notification now
			if tsk.Result == task.AlertResult {
				b, _ := json.MarshalIndent(tsk, "", " ")
				if err := n.Slack.Notify(string(b), slack.Critical); err != nil {
					log.Println(err)
				}
			} else { // if the task result is not an alert result add to the tasks list summary
				tasks = append(tasks, tsk)
			}
		case <-sendChan:
			// prepare message
			m := make(map[string]*alertStat) // [task:job]message
			fPath := tmpl.Parse(n.ReportPath, time.Now())
			writer, err := file.NewWriter(fPath, n.file)
			if err != nil {
				log.Println(err)
			}
			for _, tsk := range tasks {
				writer.WriteLine(tsk.JSONBytes())

				meta, _ := url.ParseQuery(tsk.Meta)
				key := tsk.Type + ":" + meta.Get("job")
				v, found := m[key]
				if !found {
					v = &alertStat{key: key, times: make([]time.Time, 0)}
					m[key] = v
				}
				v.count++
				v.times = append(v.times, tmpl.TaskTime(tsk))
			}

			var s string
			for k, v := range m {
				s += fmt.Sprintf("%-35s%5d  %v\n", k, v.count, tmpl.PrintDates(v.times))
			}
			if err := writer.Close(); err == nil && fPath != "" {
				s += "see report at " + fPath
			}
			fmt.Println(s)
			if err := n.Slack.Notify(s, slack.Critical); err != nil {
				log.Println(err)
			}

			tasks = tasks[0:0] // reset slice
		case <-ctx.Done():
			return
		}
	}

}

type alertStat struct {
	key   string
	count int
	times []time.Time
}

// jitterPercent will return a time.Duration representing extra
// 'jitter' to be added to the wait time. Jitter is important
// in retry events since the original cause of failure can be
// due to too many jobs being processed at a time.
//
// By adding some jitter the retry events won't all happen
// at once but will get staggered to prevent the problem
// from happening again.
//
// 'p' is a percentage of the wait time. Duration returned
// is a random duration between 0 and p. 'p' should be a value
// between 0-100.
func jitterPercent(wait time.Duration, p int64) time.Duration {
	// p == 40
	if wait == 0 {
		return 0
	}
	maxJitter := (int64(wait) * p) / 100

	return time.Duration(rand.Int63n(maxJitter))
}
