package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/davecgh/go-spew/spew"
	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task/bus"
)

const (
	name        = "crontask"
	description = `crontask task master creates tasks based on cron expression. 

A cron expression represents a set of times, using 6 space-separated fields.
Field | Field name   | Allowed values  | Allowed special characters
----- | ----------   | --------------  | --------------------------
 1    | Seconds      | 0-59            | * / , -
 2    | Minutes      | 0-59            | * / , -
 3    | Hours        | 0-23            | * / , -
 4    | Day of month | 1-31            | * / , - ?
 5    | Month        | 1-12 or JAN-DEC | * / , -
 6    | Day of week  | 0-6 or SUN-SAT  | * / , - ?`
)

func main() {
	opts := &options{
		Options: bus.NewOptions(""),
	}
	bootstrap.NewUtility(name, opts).Version(tools.String()).Description(description).Initialize()

	// producer
	p, err := bus.NewProducer(opts.Options)
	if err != nil {
		log.Fatal(err)
	}

	// cron
	c, err := makeCron(opts.Rules, p)
	if err != nil {
		log.Fatal(err)
	}
	sigChan := make(chan os.Signal, 1) // app signal handling
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	c.Start()

	// wait for shutdown signal
	<-sigChan

	// shutdown
	c.Stop()
	p.Stop()

}

type options struct {
	*bus.Options `toml:"bus"`

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

func (o options) Validate() error {
	errs := appenderr.New()
	if len(o.Rules) == 0 {
		return errors.New("at least one rule is required")
	}
	for i, r := range o.Rules {
		if r.Topic == "" && r.TaskType == "" {
			errs.Add(fmt.Errorf("topic is required: [%d]\n%s", i, spew.Sdump(r)))
		}

		if strings.Count(strings.Trim(r.CronRule, " "), " ") < 5 {
			errs.Add(fmt.Errorf("invalid cron rule: [%d]\n%s", i, spew.Sdump(r)))
		}
	}
	return errs.ErrOrNil()
}
