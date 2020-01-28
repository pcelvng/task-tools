package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/jbsmith7741/go-tools/appenderr"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task/bus"
	"github.com/robfig/cron/v3"
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
	app := bootstrap.NewUtility(name, opts).
		Version(tools.String()).
		Description(description)
	app.Initialize()
	app.AddInfo(opts.Info, opts.Port)

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
	Port         int `toml:"status_port"`
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

		// Parse the cron rules with the same rules in older versions.
		_, err := cron.NewParser(
			cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		).Parse(r.CronRule)
		if err != nil {
			errs.Add(fmt.Errorf("invalid cron rule: [%d] %s\n  error: %s", i, r.CronRule, err))
		}

	}
	return errs.ErrOrNil()
}

type NextRun struct {
	Topic string    `json:"topic"`
	Rule  string    `json:"rule"`
	In    string    `json:"run_in"`
	Time  time.Time `json:"time"`
}

func (o *options) Info() interface{} {
	fmt.Println("called options.info()")
	info := make([]NextRun, len(o.Rules))
	now := time.Now()
	for i, r := range o.Rules {
		s, err := cron.NewParser(
			cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		).Parse(r.CronRule)
		if err != nil {
			fmt.Println("error parsing cron rule", err)
		}

		info[i].Topic = r.TaskType
		info[i].Rule = r.CronRule
		info[i].Time = s.Next(now)
		info[i].In = info[i].Time.Sub(now).String()
	}
	return struct {
		Rules []NextRun `json:"rules"`
	}{
		Rules: info,
	}
}
