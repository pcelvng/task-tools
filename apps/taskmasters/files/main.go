package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

var (
	configPth = flag.String("config", "config.toml", "relative or absolute file path")
	sigChan   = make(chan os.Signal, 1) // app signal handling
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// set appOpt
	appOpt, err := loadAppOptions()
	if err != nil {
		return err
	}

	// task master
	ctx, cncl := context.WithCancel(context.Background())
	tskMstr, err := newTskMaster(appOpt)
	if err != nil {
		return err
	}
	doneCtx := tskMstr.DoFileWatch(ctx)

	select {
	case <-sigChan:
		cncl()
		<-doneCtx.Done() // wait for taskmaster to shutdown
	case <-doneCtx.Done():
		// done of its own accord
		// can be done of its own accord if
		// using a file bus.
	}

	return err
}

func newOptions() *options {
	return &options{
		Options: task.NewBusOptions(""),
	}
}

type options struct {
	*bus.Options `toml:"bus"` // bus options
	TmpDir       string       // tmp dir where in-process file objects are written.

	Rules []Rule `toml:"rule"`
}

type Rule struct {
	TaskType   string `toml:"type"`        // task type - also default topic
	SrcPattern string `toml:"src_pattern"` // source file path pattern to match (supports glob style matching)
	Topic      string `toml:"topic"`       // topic override (task type is default)

	// checks for rules that checks on groups of files instead of responding
	// immediately to an individual file.
	CronCheck  string `toml:"cron_check"`  // optional cron parsable string representing when to check src pattern matching files
	CountCheck int    `toml:"count_check"` // optional int representing how many files matching that rule to wait for until the rule is exercised
}

// loadAppOptions loads the applications
// options and sets those options to the
// global appOpt variable.
func loadAppOptions() (*options, error) {
	flag.Parse()
	opt := newOptions()

	// parse toml first - override with flag values
	_, err := toml.DecodeFile(*configPth, opt)
	if err != nil {
		return nil, err
	}

	return opt, nil
}
