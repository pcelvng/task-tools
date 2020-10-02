package main

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jbsmith7741/go-tools/appenderr"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task/bus"
)

const (
	appName     = "filewatcher"
	description = `creates tasks for new files that appear in a watched folder
use {WATCH_FILE} for templating file path into the info string for a new task

example rule:
	frequency = "1h"
	task_template = "{WATCH_FILE}?&param=other-param&dest=gs://folder/{HOUR_SLUG}/file.json"
	lookback = 24
	path_template = "gs://folder/{HOUR_SLUG}/*.json"`
)

type options struct {
	Bus *bus.Options `toml:"bus"`

	AccessKey  string  `toml:"access_key" desc:"secret token for S3/GCS access "`
	SecretKey  string  `toml:"secret_key" desc:"secret key for S3/GCS access "`
	FilesTopic string  `toml:"files_topic" desc:"topic override (default is files) disable with -"`
	TaskTopic  string  `toml:"task_topic" desc:"topic to send new task"`
	Rules      []*Rule `toml:"rule"`
}

type Rule struct {
	HourLookback int    `toml:"lookback" desc:"the number of hours for looking back for files in previous directory default: 24"`
	PathTemplate string `toml:"path_template" desc:"source file path pattern to match (supports glob style matching)"`
	Frequency    string `toml:"frequency" desc:"the wait time between checking for new files in the path_template"`
	TaskTemplate string `toml:"task_template" desc:"the template for the info string to send to the info_topic"`
}

func (o options) Validate() error {
	errs := appenderr.New()
	if o.AccessKey == "" || o.SecretKey == "" {
		log.Println("AWS Credentials are blank")
	}
	if len(o.Rules) == 0 {
		errs.Add(errors.New("at least one rule is required"))
	}
	if o.FilesTopic == "" {
		errs.Add(errors.New("file topic is required"))
	}
	return errs.ErrOrNil()
}

func main() {
	opt := &options{
		Bus:        bus.NewOptions(""),
		FilesTopic: "files",
		Rules: []*Rule{
			{
				HourLookback: 24,
				PathTemplate: "gs://folder/{HOUR_SLUG}/*.json",
				Frequency:    "1h",
				TaskTemplate: "{FILE_PATH}?&param=other-param&dest=gs://folder/{HOUR_SLUG}/file.json",
			},
		},
	}

	bootstrap.NewUtility(appName, opt).
		Description(description).
		Version(tools.String()).Initialize()

	if err := opt.Validate(); err != nil {
		log.Fatal(err)
	}
	sigChan := make(chan os.Signal, 1) // app signal handling
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	watchers, err := newWatchers(opt)
	if err != nil {
		log.Fatal(err)
	}

	for i := range watchers {
		go func(index int) {
			err := watchers[index].runWatch()
			if err != nil {
				log.Println(err)
			}
		}(i)
	}

	select {
	case <-sigChan:
		log.Println("closing...")

		if err = closeWatchers(watchers); err != nil {
			log.Fatal(err)
		}

	}
}
