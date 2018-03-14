package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

type options struct {
	*bus.Options // bus options

	AWSAccessKey string  `toml:"aws_access_key" desc:"aws secret token for S3 access "`
	AWSSecretKey string  `toml:"aws_secret_key" desc:"aws secret key for S3 access "`
	FilesTopic   string  `toml:"topic" desc:"topic override (default is files)"`
	Rules        []*Rule `toml:"rule"`
}

type Rule struct {
	HourLookback int    `toml:"lookback" desc:"the number of hours for looking back for files in previous directory default: 24"`
	PathTemplate string `toml:"path_template" desc:"source file path pattern to match (supports glob style matching)"`
	Frequency    string `toml:"frequency" desc:"the wait time between checking for new files in the path_template"`
}

var (
	configPth = flag.String("config", "config.toml", "relative or absolute file path")
	sigChan   = make(chan os.Signal, 1) // app signal handling

	defaultTopic = "files"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// app options
	appOpt, err := loadAppOptions()
	if err != nil {
		return errors.New(fmt.Sprintf("config: '%v'\n", err.Error()))
	}

	watchers, err := newWatchers(appOpt)
	if err != nil {
		return err
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

		err = closeWatchers(watchers)
		return err
	}

}

func newOptions() *options {
	return &options{
		Options: task.NewBusOptions(""),
	}
}

// loadAppOptions loads the applications
// options and sets those options to the
// global appOpt variable.
func loadAppOptions() (*options, error) {
	flag.Parse()
	opt := newOptions()
	opt.FilesTopic = defaultTopic

	// parse toml first - override with flag values
	_, err := toml.DecodeFile(*configPth, opt)
	if err != nil {
		return nil, err
	}

	return opt, nil
}
