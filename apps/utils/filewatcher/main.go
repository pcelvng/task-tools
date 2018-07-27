package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task/bus"
)

const (
	appName     = "filewatcher"
	description = ``
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

func main() {
	opt := &options{
		Bus:        bus.NewOptions(""),
		FilesTopic: "files",
	}

	bootstrap.NewUtility(appName, opt).
		Description(description).
		Version(tools.String()).Initialize()

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
