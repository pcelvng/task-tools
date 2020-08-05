package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/dustin/go-humanize"
	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"google.golang.org/api/option"
)

type worker struct {
	task.Meta
	options

	Destination `uri:"dest_table" required:"true"`
	File        string            `uri:"origin" required:"true"`
	Truncate    bool              `uri:"truncate"`
	DeleteMap   map[string]string `uri:"delete"` // will replace the data by removing current data
}

func (o *options) NewWorker(info string) task.Worker {
	w := &worker{
		Meta:    task.NewMeta(),
		options: *o,
	}
	err := uri.Unmarshal(info, w)
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	if len(w.DeleteMap) > 0 && w.Truncate {
		return task.InvalidWorker("truncate and delete options must be selected independently")
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
	bqRef.MaxBadRecords = 1

	loader := client.Dataset(w.Dataset).Table(w.Table).LoaderFrom(bqRef)
	loader.WriteDisposition = bigquery.WriteAppend
	if len(w.DeleteMap) > 0 {
		q := delStatement(w.DeleteMap, w.Destination)
		j, err := client.Query(q).Run(ctx)
		if err != nil {
			return task.Failf("delete statement: %s", err)
		}
		status, err := j.Wait(ctx)
		if err != nil {
			return task.Failf("delete wait: %s", err)
		}
		if status.Err() != nil {
			return task.Failf("delete: %s", err)
		}
		status = j.LastStatus()
		if qSts, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {
			w.SetMeta("rows_del", strconv.FormatInt(qSts.NumDMLAffectedRows, 10))
		}
	}

	if w.Truncate {
		loader.WriteDisposition = bigquery.WriteTruncate
	}

	job, err := loader.Run(ctx)
	if err != nil {
		return task.Failf("loader run: %s", err)
	}
	status, err := job.Wait(ctx)
	if err == nil {
		if status.Err() != nil {
			return task.Failf("job completed with error: %v", status.Err())
		}
		if sts, ok := status.Statistics.Details.(*bigquery.LoadStatistics); ok {
			w.SetMeta("rows_insert", strconv.FormatInt(sts.OutputRows, 10))
			return task.Completed("%d rows (%s) loaded", sts.OutputRows, humanize.Bytes(uint64(sts.OutputBytes)))
		}
	}

	return task.Completed("completed")
}

func delStatement(m map[string]string, d Destination) string {
	s := make([]string, 0)
	for k, v := range m {
		s = append(s, k+" = "+v)
	}
	sort.Sort(sort.StringSlice(s))
	return fmt.Sprintf("delete from `%s.%s.%s` where %s", d.Project, d.Dataset, d.Table, strings.Join(s, " and "))
}
