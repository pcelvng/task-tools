package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hydronica/go-config"
	"github.com/pcelvng/task/bus"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/file"
)

const (
	name        = "flowlord"
	description = name + ` creates tasks based on cron expression. 

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

type options struct {
	Workflow    string        `toml:"workflow" comment:"path to workflow file or directory"`
	Refresh     time.Duration `toml:"refresh" comment:"the workflow changes refresh duration value default is 15 min"`
	DoneTopic   string        `toml:"done_topic" comment:"default is done"`
	FileTopic   string        `toml:"file_topic" comment:"file topic for file watching"`
	FailedTopic string        `toml:"failed_topic" comment:"all retry failures published to this topic default is retry-failed, disable with '-'"`
	Port        int           `toml:"status_port"`
	Slack       *Notification `toml:"slack"`
	Bus         bus.Options   `toml:"bus"`
	File        *file.Options `toml:"file"`
}

func main() {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)
	opts := &options{
		Refresh:     time.Minute * 15,
		DoneTopic:   "done",
		FailedTopic: "retry-failed",
		File:        file.NewOptions(),
		Slack:       &Notification{},
	}

	config.New(opts).Version(tools.String()).Description(description).LoadOrDie()
	tm := New(opts)
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	// do tasks
	go func() {
		<-sigChan
		cancel()

	}()

	if err := tm.Run(ctx); err != nil {
		log.Fatal(err)
	}

}

func (o options) Validate() error {
	if o.Workflow == "" {
		return errors.New("workflow path is required")
	}

	return nil
}
