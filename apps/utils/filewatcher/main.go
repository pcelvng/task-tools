package main

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task/bus"
)

const (
	appName     = "filewatcher"
	description = `creates tasks for new files that appear in a watched folder`
)

type options struct {
	Bus *bus.Options `toml:"bus"`

	AWSAccessKey string  `toml:"aws_access_key" desc:"aws secret token for S3 access "`
	AWSSecretKey string  `toml:"aws_secret_key" desc:"aws secret key for S3 access "`
	FilesTopic   string  `toml:"files_topic" desc:"topic override (default is files)"`
	Rules        []*Rule `toml:"rule"`
}

type Rule struct {
	HourLookback int    `toml:"lookback" desc:"the number of hours for looking back for files in previous directory default: 24"`
	PathTemplate string `toml:"path_template" desc:"source file path pattern to match (supports glob style matching)"`
	Frequency    string `toml:"frequency" desc:"the wait time between checking for new files in the path_template"`
}

func (o options) Validate() error {
	errs := appenderr.New()
	if o.AWSAccessKey == "" || o.AWSSecretKey == "" {
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

	for i, _ := range watchers {
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
