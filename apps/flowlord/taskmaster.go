package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gtools "github.com/jbsmith7741/go-tools"
	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/robfig/cron/v3"

	"github.com/pcelvng/task-tools/apps/flowlord/sqlite"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/slack"
	"github.com/pcelvng/task-tools/tmpl"
)

var cronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

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
	taskCache     *sqlite.SQLite
	HostName      string
	port          int
	cron          *cron.Cron
	notify        *Notification `toml:"slack"`
	files         []fileRule

	alerts     chan task.Task
	httpServer *http.Server
}

type Notification struct {
	slack.Slack
	//ReportPath   string
	MinFrequency   time.Duration
	MaxFrequency   time.Duration
	alertFrequency atomic.Int64 // summary backoff; same value as used in Tick (GetAlertFrequency)

	// Alert loop state (owned by handleNotifications goroutine)
	lastAlertTime time.Time // batch watermark: alerts with created_at after this are candidates
	lastRun       time.Time // watermark for new rows since last Tick (backoff churn)

	file *file.Options
}

// GetAlertFrequency returns the current notification duration
func (n *Notification) GetAlertFrequency() time.Duration {
	return time.Duration(n.alertFrequency.Load())
}

// setAlertFrequency atomically sets the current notification duration
func (n *Notification) setAlertFrequency(d time.Duration) {
	n.alertFrequency.Store(int64(d))
}

// initAlertLoopState resets watermarks for a new notification loop (local start-of-day batch anchor, tick anchor now).
func (n *Notification) initAlertLoopState() {
	now := time.Now()
	n.lastAlertTime = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	n.lastRun = time.Now()
	n.setAlertFrequency(n.MinFrequency)
}

// Tick runs one poll: incomplete tasks, optional summary via sendSummary, then adjust backoff from new rows since lastRun.
func (n *Notification) Tick(taskCache *sqlite.SQLite, sendSummary func([]sqlite.AlertRecord) error) {
	taskCache.CheckIncompleteTasks()

	pending, err := taskCache.GetAlertsAfterTime(n.lastAlertTime)
	if err != nil {
		log.Printf("failed to retrieve alerts: %v", err)
		return
	}
	waitTime := n.GetAlertFrequency()
	if len(pending) > 0 && time.Since(n.lastAlertTime) >= waitTime {
		if err := sendSummary(pending); err != nil {
			log.Println(err)
		}
		n.lastAlertTime = time.Now()
	}

	newAlerts, err := taskCache.GetAlertsAfterTime(n.lastRun)
	if err != nil {
		log.Printf("failed to retrieve alerts since last tick: %v", err)
		n.lastRun = time.Now()
		return
	}

	if len(newAlerts) == 0 && waitTime > n.MinFrequency {
		next := waitTime / 2
		if next < n.MinFrequency {
			waitTime = n.MinFrequency
		} else {
			waitTime = next
		}
		log.Println("de-escalate waitTime ", waitTime)
	} else if len(newAlerts) > 0 && waitTime < n.MaxFrequency {
		next := waitTime * 2
		if next > n.MaxFrequency {
			waitTime = n.MaxFrequency
		} else {
			waitTime = next
		}
		log.Println("escalate waitTime ", waitTime)
	}
	n.setAlertFrequency(waitTime)
	n.lastRun = time.Now()
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
	if err := opts.DB.Open(opts.Workflow, opts.File); err != nil {
		log.Fatal("db init", err)
	}

	opts.Slack.file = opts.File
	// Initialize current duration to MinFrequency
	opts.Slack.setAlertFrequency(opts.Slack.MinFrequency)
	tm := &taskMaster{
		initTime:     time.Now(),
		taskCache:    opts.DB,
		path:         opts.Workflow,
		doneTopic:    opts.DoneTopic,
		failedTopic:  opts.FailedTopic,
		fOpts:        opts.File,
		producer:     producer,
		doneConsumer: consumer,
		port:         opts.Port,
		HostName:     opts.Host,
		cron:         cron.New(cron.WithParser(cronParser)),
		dur:          opts.Refresh,
		notify:       opts.Slack,
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
	for _, c := range tm.taskCache.Children(task.Task{Type: topic, Meta: "workflow=" + workflow + "&job=" + job}) {
		job := strings.Trim(c.Topic()+":"+c.Job(), ":")
		if children := tm.getAllChildren(c.Task, workflow, c.Job()); len(children) > 0 {
			job += " ➞ " + strings.Join(children, " ➞ ")
		}
		s = append(s, job)
	}
	return s
}

func (tm *taskMaster) refreshCache() ([]string, error) {
	// Reload workflow files
	files, err := tm.taskCache.Refresh()
	if err != nil {
		return nil, fmt.Errorf("error reloading workflow: %w", err)
	}
	// if there are values in files, there are changes that need to be reloaded
	if len(files) > 0 {
		log.Println("reloading workflow changes")
		tcron := tm.cron
		tm.cron = cron.New(cron.WithParser(cronParser))
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
	// The SQLite struct now implements the workflow.Cache interface directly

	// check for alerts from today on startup	// refresh the workflow if the file(s) have been changed
	_, err = tm.refreshCache()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		workflowTick := time.NewTicker(tm.dur)
		DBTick := time.NewTicker(24 * time.Hour)
		defer workflowTick.Stop()
		defer DBTick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-DBTick.C:
				s, err := tm.taskCache.Recycle(time.Now().Add(-tm.taskCache.Retention))
				log.Printf(" cache recycle:%s err:%v", s, err)
				if err := tm.taskCache.Sync(); err != nil {
					log.Println("DB sync", err)
				}
			case <-workflowTick.C:
				if _, err := tm.refreshCache(); err != nil {
					log.Println(err)
				}
			}
		}
	}()

	if err := tm.schedule(); err != nil {
		return fmt.Errorf("cron schedule %w", err)
	}

	go tm.readDone(ctx)
	go tm.readFiles(ctx)

	if err := tm.StartHandler(); err != nil {
		return err
	}
	go tm.handleNotifications(tm.alerts, ctx)
	<-ctx.Done()
	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if tm.httpServer != nil {
		if err := tm.httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}
	tm.cron.Stop()
	return tm.taskCache.Close()
}

// schedule the tasks and refresh the schedule when updated
func (tm *taskMaster) schedule() (err error) {
	errs := make([]error, 0)

	// Get all workflow files from database
	workflowFiles := tm.taskCache.GetWorkflowFiles()

	if len(workflowFiles) == 0 {
		return fmt.Errorf("no workflows found check path %s", tm.path)
	}

	// Get all phases for each workflow file
	for _, filePath := range workflowFiles {
		phases, err := tm.taskCache.GetPhasesForWorkflow(filePath)
		if err != nil {
			errs = append(errs, fmt.Errorf("error getting phases for %s: %w", filePath, err))
			continue
		}

		for _, w := range phases {
			rules, _ := url.ParseQuery(w.Rule)
			cronSchedule := rules.Get("cron")
			if f := rules.Get("files"); f != "" {
				r := fileRule{
					SrcPattern:   f,
					workflowFile: filePath,
					Phase:        w.ToWorkflowPhase(),
					CronCheck:    cronSchedule,
				}
				r.CountCheck, _ = strconv.Atoi(rules.Get("count"))

				tm.files = append(tm.files, r)

				//todo: Create a cron job for a task that is cron and files
			}

			if cronSchedule == "" {
				//log.Printf("no cron: task:%s, rule:%s", w.Task, w.Rule)
				// this should already be in the status field
				continue
			}

			j, err := tm.NewJob(w.ToWorkflowPhase(), filePath)
			if err != nil {
				errs = append(errs, fmt.Errorf("issue with %s %w", w.Task, err))
			}

			if _, err = tm.cron.AddJob(cronSchedule, j); err != nil {
				errs = append(errs, fmt.Errorf("invalid rule for %s:%s %s %w", filePath, w.Task, w.Rule, err))
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
	switch t.Result {
	case task.WarnResult:
		return nil // do nothing
	case task.AlertResult:
		tm.alerts <- *t
	case task.ErrResult:
		p := tm.taskCache.Get(*t)
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
				// Potential loss of task if app stopped or restarted
				tm.taskCache.Add(*t)
				if err := tm.producer.Send(t.Type, t.JSONBytes()); err != nil {
					log.Println(err)
				}
			}()
			return nil
		}
		// send to the retry failed topic if retries > p.Retry
		meta.Set("retry", "failed")
		meta.Set("retried", strconv.Itoa(p.Retry))
		t.Meta = meta.Encode()
		if tm.failedTopic != "-" && tm.failedTopic != "" {
			tm.taskCache.Add(*t)
			if err := tm.producer.Send(tm.failedTopic, t.JSONBytes()); err != nil {
				return err
			}
		}

		// don't alert if slack isn't enabled or disabled in phase
		if rules.Get("no_alert") != "" {
			return nil
		}

		tm.alerts <- *t

		return nil
	case task.CompleteResult:
		// start off any children tasks
		taskTime := tmpl.TaskTime(*t)
		phases := tm.taskCache.Children(*t)
		for _, p := range phases {

			if !isReady(p.Rule, t.Meta) {
				continue
			}

			// Create Batch struct for potential expansion
			batch := Batch{
				Template: p.Template,
				Task:     p.Topic(),
				Job:      p.Job(),
				Workflow: meta.Get("workflow"),
				// By:       rules.Get("by"),
				Meta: Meta(meta), // will be replaced with meta from rules?
				// Metafile: rules.Get("meta-file"),
			}

			if err := uri.UnmarshalQuery(p.Rule, &batch); err != nil {
				log.Printf("error parsing rule %q for %s: %v", p.Rule, p.Topic(), err)
			}
			// Use Batch method to generate tasks (handles single or multiple tasks)
			childTasks, err := batch.At(taskTime, tm.fOpts)
			if err != nil {
				log.Printf("error creating child tasks for %s: %v", p.Topic(), err)
				continue
			}

			// Send all generated child tasks
			for _, child := range childTasks {
				// Ensure child has the correct parent ID and meta
				child.ID = t.ID

				// Add parent meta information
				childMeta := "workflow=" + meta.Get("workflow")
				if v := meta.Get("cron"); v != "" {
					childMeta += "&cron=" + v
				}
				if child.Job != "" {
					childMeta += "&job=" + child.Job
				}
				child.Meta = childMeta

				tm.taskCache.Add(child)
				if err := tm.producer.Send(child.Type, child.JSONBytes()); err != nil {
					return err
				}
			}
		}
		if len(phases) == 0 {
			log.Printf("no matches found for %v:%v", t.Type, t.Job)
		}
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
		if err := tm.matchFile(s); err != nil {
			log.Println("files: ", err)
		}
	}
}

// handleNotifications gathers all 'failed' tasks and incomplete tasks
// and sends a Slack summary when there is pending work and waitTime has elapsed.
// Polling runs at MinFrequency; waitTime is the exponential backoff between
// summary sends (min → max: e.g. 5 → 10 → 20 …), descalated when no new alert
// rows appear since lastRun, escalated when any do.
func (tm *taskMaster) handleNotifications(taskChan chan task.Task, ctx context.Context) {
	tm.notify.initAlertLoopState()

	tm.notify.Tick(tm.taskCache, tm.sendAlertSummary)
	ticker := time.NewTicker(tm.notify.MinFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.notify.Tick(tm.taskCache, tm.sendAlertSummary)
		case tsk := <-taskChan:
			// if the task result is an alert result, send a slack notification now
			if tsk.Result == task.AlertResult {
				b, _ := json.MarshalIndent(tsk, "", " ")
				if err := tm.notify.Slack.Notify(string(b), slack.Critical); err != nil {
					log.Println(err)
				}
			} else { // if the task result is not an alert result add to the tasks list summary
				if err := tm.taskCache.AddAlert(tsk, tsk.Msg); err != nil {
					log.Printf("failed to store alert: %v", err)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// sendAlertSummary sends a formatted alert summary to Slack
// This can be reused by backup alert system and other components
func (tm *taskMaster) sendAlertSummary(alerts []sqlite.AlertRecord) error {
	if len(alerts) == 0 {
		return nil
	}

	// build compact summary using existing logic
	summary := sqlite.BuildCompactSummary(alerts)

	// format message similar to current Slack format
	var message strings.Builder
	message.WriteString(fmt.Sprintf("see report at %v:%d/web/alert?date=%s\n", tm.HostName, tm.port, time.Now().Format("2006-01-02")))

	for _, line := range summary {
		message.WriteString(fmt.Sprintf("%-35s%5d  %s\n",
			line.Key+":", line.Count, line.TimeRange))
	}

	// send to Slack if configured
	log.Println(message.String())
	if tm.notify != nil {
		if err := tm.notify.Notify(message.String(), slack.Critical); err != nil {
			return fmt.Errorf("failed to send alert summary to Slack: %w", err)
		}
	}

	return nil
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
