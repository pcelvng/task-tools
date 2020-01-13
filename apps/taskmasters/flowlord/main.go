package main

import (
	"errors"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
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
	Workflow string `comment:"path to workflow file or directory"`
	Refresh  string `comment:"the refresh duration value default is 1 hour '1h'"`
}

func main() {
	opts := &options{
		Refresh: "1h",
	}

	app := bootstrap.NewTaskMaster(name, New, opts).
		Version(tools.String()).
		Description(description)

	app.Initialize()
	app.Run()
}

func (o options) Validate() error {
	if o.Workflow == "" {
		return errors.New("workflow path is required")
	}

	return nil
}
