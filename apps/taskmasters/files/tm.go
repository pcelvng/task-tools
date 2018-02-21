package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/robfig/cron"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task/bus"
)

var (
	dumpPrefix = "tm-files_"
)

func newTskMaster(appOpt *options) (*tskMaster, error) {
	// validate
	err := validateAppOpts(appOpt)
	if err != nil {
		return nil, err
	}

	// producer
	producer, err := bus.NewProducer(appOpt.Options)
	if err != nil {
		return nil, err
	}

	// consumer
	consumer, err := bus.NewConsumer(appOpt.Options)
	if err != nil {
		return nil, err
	}

	// context to indicate tskMaster is done shutting down.
	doneCtx, doneCncl := context.WithCancel(context.Background())

	// tm
	tm := &tskMaster{
		producer: producer,
		consumer: consumer,
		appOpt:   appOpt,
		doneCtx:  doneCtx,
		doneCncl: doneCncl,
		files:    make(map[*Rule][]*stat.Stats),
		msgCh:    make(chan *stat.Stats),
		rules:    appOpt.Rules,
		l:        log.New(os.Stderr, "", log.LstdFlags),
	}

	// make cron
	tm.c, err = makeCron(appOpt.Rules, tm)
	if err != nil {
		return nil, err
	}

	return tm, nil
}

func validateAppOpts(appOpt *options) error {
	if len(appOpt.Rules) == 0 {
		return errors.New("no rules provided")
	}

	// validate each rule
	for _, rule := range appOpt.Rules {
		if rule.TaskType == "" {
			return errors.New("task type required for all rules")
		}

		if rule.SrcPattern == "" {
			return errors.New("src_pattern required for all rules")
		}
	}

	return nil
}

// tskMaster is the main application runtime
// object that will watch for files
// and apply the config rules.
type tskMaster struct {
	producer   bus.Producer
	consumer   bus.Consumer
	appOpt     *options
	doneCtx    context.Context         // communicate app has shut down
	doneCncl   context.CancelFunc      // communicate app has shut down
	finishCncl context.CancelFunc      // internally indicate taskmaster needs to shutdown
	files      map[*Rule][]*stat.Stats // stats files associated with one or more rules stored for later.
	msgCh      chan *stat.Stats
	rules      []*Rule // a complete list of rules
	l          *log.Logger
	c          *cron.Cron

	mu sync.Mutex
	wg sync.WaitGroup
}

// DoWatch will accept a context for knowing if/when
// it should perform a shutdown. A context is returned
// to allow the caller to know when shutdown is complete.
func (tm *tskMaster) DoFileWatch(ctx context.Context) context.Context {
	// allow for internally initiated shutdown
	ctx, tm.finishCncl = context.WithCancel(ctx)

	go tm.readFileStats(ctx) // read file stats messages
	go tm.doWatch(ctx)       // start doing

	return tm.doneCtx
}

func (tm *tskMaster) doWatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			tm.consumer.Stop()
			tm.wg.Wait()       // wait for in-bound messages to process
			tm.clearFiles()    // flush out counts and cron
			tm.producer.Stop() // starve msgs

			// signal a completed shutdown
			tm.doneCncl()
			return
		case sts := <-tm.msgCh:
			tm.matchAll(sts)
		}
	}
}

// readFileStats will read files off the msg bus
// and push them to the msgCh.
func (tm *tskMaster) readFileStats(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, done, err := tm.consumer.Msg()
			if err != nil {
				tm.l.Println(err.Error())
			}

			if len(msg) > 0 {
				sts := stat.NewFromBytes(msg)
				tm.msgCh <- &sts
			}

			if done && ctx.Err() == nil {
				// wait for outstanding messages to process
				tm.wg.Wait()

				// clear out remaining bucket items
				tm.waitClearFiles()

				// send internal shutdown message
				tm.finishCncl()
			}
		}
	}
}

// clearFiles will clear out count and cron based
// rules and send out the corresponding tasks.
func (tm *tskMaster) clearFiles() {
	// clear out count based matches immediately
	for rule, rSts := range tm.files {
		if len(rSts) == 0 {
			continue
		}

		tm.sendDirTsks(rule)
	}
}

// clearFiles will wait for cron tasks to
// clear out via cron and clear out CountCheck
// type rules immediately.
func (tm *tskMaster) waitClearFiles() {
	// clear out count based matches immediately
	for rule, rSts := range tm.files {
		// skip cron rules
		if rule.CronCheck != "" {
			continue
		}

		if len(rSts) == 0 {
			continue
		}

		tm.sendDirTsks(rule)
	}

	// wait for cron rules to clear remaining
	for !tm.isFilesEmpty() {
		tkr := time.NewTicker(time.Second)
		<-tkr.C
	}
}

func (tm *tskMaster) isFilesEmpty() bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for rule, rSts := range tm.files {
		// skip cron rules
		if rule.CronCheck == "" {
			continue
		}

		if len(rSts) > 0 {
			return false
		}
	}

	return true
}

// matchAll will discover if the file matches one or more rules
// and either responds immediately (for non-batch or max count rules)
// or stores the stats for a later response (for cron rules).
func (tm *tskMaster) matchAll(sts *stat.Stats) {
	tm.wg.Add(1)
	defer tm.wg.Done()

	for _, rule := range tm.rules {
		tm.match(sts, rule)
	}
}

func (tm *tskMaster) match(sts *stat.Stats, rule *Rule) {
	if isMatch, _ := filepath.Match(rule.SrcPattern, sts.Path); !isMatch {
		return
	}

	// goes to a rule bucket?
	if rule.CountCheck > 0 || rule.CronCheck != "" {
		tm.addSts(rule, sts)

		// count check - send tsk if count is full
		if rule.CountCheck > 0 {
			tm.countCheck(rule)
		}
	} else {
		// does not go to a rule bucket so
		// create task and send immediately
		tsk := task.New(rule.TaskType, sts.InfoString())
		tm.sendTsk(tsk, rule)
	}
}

// addSts will add the file stats to the corresponding rule bucket.
func (tm *tskMaster) addSts(rule *Rule, sts *stat.Stats) {
	if rule.CountCheck > 0 || rule.CronCheck != "" {
		tm.mu.Lock()
		defer tm.mu.Unlock()

		if tm.files[rule] == nil {
			tm.files[rule] = []*stat.Stats{sts}
		}
		tm.files[rule] = append(tm.files[rule], sts)
	}
}

// countCheck - if the count is high enough
// then flush the sts and create a corresponding task.
func (tm *tskMaster) countCheck(rule *Rule) {
	if rule.CountCheck > 0 && tm.lenFiles(rule) >= rule.CountCheck {
		// it's possible that between the len check and getting
		// the pthDirs a cron rule was activated and fileStats was
		// already called for this rule and cleared out. In this
		// case nothing will be sent unless also it happens that
		// a new file stats was also added. In which case, if
		// a task with the same pthDir is sent, the worker will
		// do the same job twice within a short period of time or
		// the tasks will be balanced between two workers if there
		// are multiple instances.
		tm.sendDirTsks(rule)
	}
}

// lenFiles returns the number of file stats associated with that rule
func (tm *tskMaster) lenFiles(rule *Rule) uint {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	return uint(len(tm.files[rule]))
}

// fileStats will return a unique list of file directories stashed
// with that rule and then clear all stats associated with that rule.
func (tm *tskMaster) fileStats(rule *Rule) (pthDirs []string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// make unique list of pthDirs
	pthDirsMp := make(map[string]int)
	for _, sts := range tm.files[rule] {
		pthDir, _ := path.Split(sts.Path)
		pthDirsMp[pthDir] = 0
	}

	// translate unique map list to slice
	for pthDir := range pthDirsMp {
		pthDirs = append(pthDirs, pthDir)
	}

	// clear stats
	tm.files[rule] = make([]*stat.Stats, 0)

	return pthDirs
}

// sendDirTsks will create tasks for each unique
// path directory. The file stats associated with
// that rule will be cleared.
func (tm *tskMaster) sendDirTsks(rule *Rule) {
	pthDirs := tm.fileStats(rule)

	// create and send tasks
	for _, pthDir := range pthDirs {
		tsk := task.New(rule.TaskType, pthDir) // pthDir == info
		tm.sendTsk(tsk, rule)
	}
}

func (tm *tskMaster) sendTsk(tsk *task.Task, rule *Rule) {
	topic := rule.TaskType
	if rule.Topic != "" {
		topic = rule.Topic
	}

	err := tm.producer.Send(topic, tsk.JSONBytes())
	if err != nil {
		tm.l.Printf("send on topic '%v' msg '%v'", topic, err.Error())
	}
}

// makeCron will create the cron and setup all the cron jobs.
// It will also start the cron if there are no errors and if there
// is at least one job.
func makeCron(rules []*Rule, tm *tskMaster) (*cron.Cron, error) {
	c := cron.New()
	for _, rule := range rules {
		if rule.CronCheck == "" {
			continue
		}

		job := newJob(rule, tm)
		err := c.AddJob(rule.CronCheck, job)
		if err != nil {
			return nil, fmt.Errorf("invalid cron: '%v'", err.Error())
		}
	}
	c.Location()
	c.Start()

	return c, nil
}

func newJob(rule *Rule, tm *tskMaster) *job {
	return &job{
		rule: rule,
		tm:   tm,
	}
}

type job struct {
	rule *Rule
	tm   *tskMaster
}

func (j *job) Run() {
	j.tm.sendDirTsks(j.rule)
}
