package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task/bus"
)

var (
	dumpPrefix = "tsk-files_"
)

func newTskMaster(appOpt *options) (*tskMaster, error) {
	// files bus
	b, err := bus.NewBus(appOpt.Options)
	if err != nil {
		return nil, err
	}

	// logger
	l := log.New(os.Stderr, "", log.LstdFlags)

	// read-in locally dumped file objects
	// in case app was shut down.

	// context to indicate tskMaster is done shutting down.
	doneCtx, doneCncl := context.WithCancel(context.Background())

	return &tskMaster{
		b:        b,
		appOpt:   appOpt,
		doneCtx:  doneCtx,
		doneCncl: doneCncl,
		files:    make(map[*Rule][]*stat.Stats),
		msgCh:    make(chan *stat.Stats),
		rules:    appOpt.Rules,
		l:        l,
	}, nil
}

// tskMaster is the main application runtime
// object that will watch for files
// and apply the config rules.
type tskMaster struct {
	b        *bus.Bus // files bus
	appOpt   *options
	doneCtx  context.Context
	doneCncl context.CancelFunc
	files    map[*Rule][]*stat.Stats // stats files associated with one or more rules stored for later.
	msgCh    chan *stat.Stats
	rules    []Rule // a complete list of rules
	l        *log.Logger

	mu sync.Mutex
}

// DoWatch will accept a context for knowing if/when
// it should perform a shutdown. A context is returned
// to allow the caller to know when shutdown is complete.
func (tm *tskMaster) DoFileWatch(ctx context.Context) context.Context {
	// start doing
	go tm.doWatch(ctx)

	return tm.doneCtx
}

func (tm *tskMaster) doWatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			tm.dumpFiles()
			return
		case sts := <-tm.msgCh:
			tm.matchAll(sts)
		}
	}
}

// matchAll will discover if the file matches one or more rules
// and either responds immediately (for non-batch or max count rules)
// or stores the stats for a later response (for cron rules).
func (tm *tskMaster) matchAll(sts *stat.Stats) {
	for _, rule := range tm.rules {
		if isMatch, _ := filepath.Match(rule.SrcPattern, sts.Path); !isMatch {
			continue
		}

		// goes to a rule bucket?
		if rule.CountCheck > 0 || rule.CronCheck != "" {
			tm.addSts(&rule, sts)

			// count check - send tsk if count is full
			if rule.CountCheck > 0 {
				tm.countCheck(&rule, sts)
			}
		} else {
			// does not go to a rule bucket so
			// create task and send immediately
			tsk := task.New(rule.TaskType, sts.InfoString())
			tm.sendTsk(tsk, &rule)
		}
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
			tm.countCheck(rule, sts)
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
func (tm *tskMaster) countCheck(rule *Rule, sts *stat.Stats) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if rule.CountCheck > 0 && len(tm.files[rule]) >= rule.CountCheck {
		// create task and send
		pthDir, _ := path.Split(sts.Path)
		tsk := task.New(rule.TaskType, pthDir) // pthDir == info here
		tm.sendTsk(tsk, rule)

		// flush stats
		tm.files[rule] = make([]*stat.Stats, 0)
	}
}

func (tm *tskMaster) sendTsk(tsk *task.Task, rule *Rule) {
	topic := rule.TaskType
	if rule.Topic != "" {
		topic = rule.Topic
	}

	err := tm.b.Send(topic, tsk.JSONBytes())
	if err != nil {
		tm.l.Printf("send: on topic '%v' got '%v'", topic, err.Error())
	}
}

// dumpFiles will write all in-progress to a tmp file.
// For simplicity dumpFiles just writes the json file objects
// to the default os.TmpDir with  prefix.
func (tm *tskMaster) dumpFiles() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// create unique map (since there can be dups)
	allSts := make(map[string]*stat.Stats)
	for _, stsSet := range tm.files {
		for _, sts := range stsSet {
			// the key is over-kill in case Created
			// or Checksum is not present.
			key := sts.Path + sts.Created + sts.Checksum
			allSts[key] = sts
		}
	}

	// don't try writing if there is nothing to write
	// that way an empty tmp file isn't created.
	if len(allSts) == 0 {
		return
	}

	// create file bfr
	opt := buf.NewOptions()
	opt.UseFileBuf = true
	opt.FileBufDir = tm.appOpt.TmpDir
	opt.FileBufPrefix = dumpPrefix
	bfr, err := buf.NewBuffer(opt)
	if err != nil {
		// log error and return
		tm.l.Printf("files dump: '%v'", err.Error())
		return
	}

	// write to tmp file
	for _, sts := range allSts {
		bfr.WriteLine(sts.JSONBytes())
	}
	bfr.Close() // flush to file

	return
}

// readinFiles will access the tmp dir and read in
// tmp files. Default is os.TmpDir.
func readinFiles(tmpDir string) ([]stat.Stats, error) {
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}

	// find matching tmp files
	glbPth := path.Join(tmpDir, dumpPrefix)
	glbPth = fmt.Sprintf("%v*", glbPth)
	pths, err := filepath.Glob(glbPth)
	if err != nil {
		return nil, err
	}

	// read in records
	allSts := make([]stat.Stats, 0)
	for _, pth := range pths {
		// reader
		r, err := file.NewReader(pth, nil)
		if err != nil {
			return nil, err
		}

		// read in file
		for {
			ln, err := r.ReadLine()
			if len(ln) > 0 {
				sts := stat.NewFromBytes(ln)
				if sts.Path != "" {
					allSts = append(allSts, sts)
				}
			}

			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
		}
	}

	// rm existing files
	for _, pth := range pths {
		os.Remove(pth)
	}

	return allSts, nil
}
