package main

import (
	_ "embed"
	"errors"
	"strings"

	"cloud.google.com/go/bigquery"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
)

const taskType = "bigquery"

//go:embed README.md
var desc string

type options struct {
	BqAuth  string       `toml:"bq_auth" comment:"file path to service file"`
	Project string       `toml:"project"`
	Fopts   file.Options `toml:"file"`
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
		return errors.New("requires (project.dataset.table)")
	}

	d.Project, d.Dataset, d.Table = l[0], l[1], l[2]
	return nil
}

func (d *Destination) IsZero() bool {
	return d.Project == "" && d.Dataset == "" && d.Table == ""
}

func (d Destination) String() string {
	return d.Project + "." + d.Dataset + "." + d.Table
}

func (d Destination) BqTable(client *bigquery.Client) *bigquery.Table {
	return client.DatasetInProject(d.Project, d.Dataset).Table(d.Table)
}
