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

	// app options
	appOpt, err := loadAppOptions()
	if err != nil {
		return errors.New(fmt.Sprintf("config: '%v'\n", err.Error()))
	}

	// producer
	p, err := bus.NewProducer(appOpt.Options)
	if err != nil {
		return err
	}

	// cron
	c, err := makeCron(appOpt.Rules, p)
	if err != nil {
		return err
	}
	c.Start()

	// wait for shutdown signal
	<-sigChan

	// shutdown
	c.Stop()
	p.Stop()

	return err
}

func newOptions() *options {
	return &options{
		Options: bus.NewOptions(""),
	}
}

type options struct {
	*bus.Options

	// rules
	Rules []Rule `toml:"rule"`
}

type Rule struct {
	CronRule     string `toml:"cron"`
	TaskType     string `toml:"type"` // also default topic
	TaskTemplate string `toml:"template"`
	HourOffset   int    `toml:"offset"`
	Topic        string `toml:"topic"` // topic override
}

func loadAppOptions() (*options, error) {
	flag.Parse()
	c := newOptions()

	if _, err := toml.DecodeFile(*configPth, c); err != nil {
		return nil, err
	}
	return c, nil
}
