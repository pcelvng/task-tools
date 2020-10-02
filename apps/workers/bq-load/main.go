package main

import (
	"errors"
	"strings"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
)

const (
	taskType = "bq_load"
	desc     = `load a delimited json file into BigQuery 

info params 
 - origin: (required)  file to be loaded (gs://path/file.json)
 - destination: (required) project.dataset.table to be insert into
 - truncate: truncate the table (delete ALL and insert). Default behavior is to append data 
 - delete: map field defines the column and values to delete before inserting (delete=id:10|date:2020-01-02)


example for file reader:
{"task":"bq_load", "info":"gs://my/data.json?dest_table=project.reports.impressions&delete=date:2020-01-02|id:11"}

example for GCS reference:
{"task":"bq_load", "info":"gs://folder/*.json?dest_table=project.reports.impressions&from_gcs=true&append=true"}`
)

type options struct {
	BqAuth string `toml:"bq_auth" comment:"file path to service file"`

	Fopts file.Options `toml:"file"`
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

func (d Destination) String() string {
	return d.Project + "." + d.Dataset + "." + d.Table
}
