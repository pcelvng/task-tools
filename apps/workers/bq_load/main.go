package main

import (
	"fmt"
	"strings"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
)

const (
	taskType = "bq_load"
	desc     = ``
)

type options struct {
	BqAuth string `toml:"bq_auth" comment:"file path to service file"`
}

func (o *options) Validate() error {
	return nil
}

func main() {
	opts := &options{}
	app := bootstrap.NewWorkerApp(taskType, opts.NewWorker, opts).
		Description(desc).
		Version(tools.Version).Initialize()

	app.Run()
}

type Destination struct {
	Project string
	Dataset string
	Table   string
}

func (d *Destination) UnmarshalText(text []byte) error {
	l := strings.Split(string(text), ".")
	if len(l) != 3 || len(l[0]) == 0 || len(l[1]) == 0 || len(l[2]) == 0 {
		return fmt.Errorf("invalid dest_table %s (project.dataset.table)" + string(text))
	}

	d.Project, d.Dataset, d.Table = l[0], l[1], l[2]
	return nil
}

func (d Destination) String() string {
	return d.Project + "." + d.Dataset + "." + d.Table
}
