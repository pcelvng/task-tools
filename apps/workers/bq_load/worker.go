package main

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/dustin/go-humanize"
	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"google.golang.org/api/option"
)

type worker struct {
	options

	Destination `uri:"dest_table" required:"true"`
	File        string `uri:"origin" required:"true"`
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

func (o *options) NewWorker(info string) task.Worker {
	w := &worker{
		options: *o,
	}
	err := uri.Unmarshal(info, w)
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	return w
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	opts := make([]option.ClientOption, 0)
	if w.BqAuth != "" {
		opts = append(opts, option.WithCredentialsFile(w.BqAuth))
	}
	client, err := bigquery.NewClient(ctx, w.Project, opts...)
	if err != nil {
		return task.Failf("bigquery client init %s", err)
	}

	bqRef := bigquery.NewGCSReference(w.File)
	bqRef.SourceFormat = bigquery.JSON
	//bqRef.Schema = bqBidWinSchema()
	bqRef.MaxBadRecords = 1

	loader := client.Dataset(w.Dataset).Table(w.Table).LoaderFrom(bqRef)
	//loader.WriteDisposition = bigquery.WriteAppend
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		return task.Failf("loader run", err)
	}
	status, err := job.Wait(ctx)
	if err == nil {
		if status.Err() != nil {
			return task.Failf("job completed with error: %v", status.Err())
		}
		if sts, ok := status.Statistics.Details.(*bigquery.LoadStatistics); ok {
			return task.Completed("%d rows (%s) loaded", sts.OutputRows, humanize.Bytes(uint64(sts.OutputBytes)))
		}

	}

	return task.Completed("completed")
}
