package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file/stat"
)

const (
	appName     = "files"
	description = ``
)

func main() {
	opts := &options{}

	bootstrap.NewTaskMaster(appName, opts.new, opts).
		Description(description).Version(tools.String()).
		Initialize().
		Run()
}

type options struct {
	Rules []*Rule `toml:"rule"`
}

func (o options) Validate() error {
	if len(o.Rules) == 0 {
		return errors.New("no rules provided")
	}

	// validate each rule
	for _, rule := range o.Rules {
		if rule.TaskType == "" {
			return errors.New("task type required for all rules")
		}

		if rule.SrcPattern == "" {
			return errors.New("src_pattern required for all rules")
		}
	}

	return nil
}

func (appOpt *options) new(app *bootstrap.Starter) bootstrap.Runner {
	doneCtx, doneCncl := context.WithCancel(context.Background())
	tm := &tskMaster{
		initTime: time.Now(),
		producer: app.NewProducer(),
		consumer: app.NewConsumer(),
		appOpt:   appOpt,
		doneCtx:  doneCtx,
		doneCncl: doneCncl,
		files:    make(map[*Rule][]*stat.Stats),
		msgCh:    make(chan *stat.Stats),
		rules:    appOpt.Rules,
		l:        log.New(os.Stderr, "", log.LstdFlags),
	}
	var err error
	// make cron
	tm.c, err = makeCron(appOpt.Rules, tm)
	if err != nil {
		log.Fatal(err)
	}
	return tm
}

type Rule struct {
	TaskType     string `toml:"type"`          // task type - also default topic
	SrcPattern   string `toml:"src_pattern"`   // source file path pattern to match (supports glob style matching)
	InfoTemplate string `toml:"info_template"` // info template (not required)
	Topic        string `toml:"topic"`         // topic override (task type is default)

	// checks for rules that checks on groups of files instead of responding
	// immediately to an individual file.
	CronCheck  string `toml:"cron_check"`  // optional cron parsable string representing when to check src pattern matching files
	CountCheck uint   `toml:"count_check"` // optional int representing how many files matching that rule to wait for until the rule is exercised
}
