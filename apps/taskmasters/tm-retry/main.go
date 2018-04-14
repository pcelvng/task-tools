package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pcelvng/task/bus"
)

var (
	config  = flag.String("config", "config.toml", "relative or absolute file path")
	sigChan = make(chan os.Signal, 1)
)

func main() {
	err := run()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
}

func run() error {
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	flag.Parse()
	conf, err := LoadConfig(*config)
	if err != nil {
		return err
	}

	// make retryer
	r, err := newRetryer(conf)
	if err != nil {
		return err
	}

	select {
	case <-sigChan:
		log.Println("closing...")

		err = r.close()
		return err
	}
}

var (
	defaultDoneTopic   = "done"
	defaultDoneChannel = "retry"
)

func newOptions() *options {
	return &options{
		Options:          bus.NewOptions(""),
		DoneTopic:        defaultDoneTopic,
		DoneChannel:      defaultDoneChannel,
		RetriedTopic:     "retried",
		RetryFailedTopic: "retry-failed",
	}
}

type options struct {
	*bus.Options

	// topic and channel to listen to
	// done tasks for retry review.
	DoneTopic        string `toml:"done_topic"`
	DoneChannel      string `toml:"done_channel"`
	RetriedTopic     string `toml:"retried_topic"`      // all retries published to this topic (disable with "-" value)
	RetryFailedTopic string `toml:"retry_failed_topic"` // all failures (retried and failed) published to this topic

	// retry rules
	RetryRules []*RetryRule `toml:"rule"`
}

type RetryRule struct {
	TaskType string   `toml:"type"`
	Retries  int      `toml:"retry"`
	Wait     duration `toml:"wait"`  // duration to wait before creating and sending new task
	Topic    string   `toml:"topic"` // topic override (default is AppName value)
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error

	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func LoadConfig(filePath string) (*options, error) {
	c := newOptions()

	if _, err := toml.DecodeFile(filePath, c); err != nil {
		return nil, err
	}

	return c, nil
}
