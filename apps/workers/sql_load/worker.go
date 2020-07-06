package main

import (
	"context"

	"github.com/pcelvng/task"
)

type InfoOptions struct {
	Strict        bool   `uri:"strict"`                // field names in json must match field names in the table
	Copy          bool   `uri:"copy"`                  // use a copy statement, and not the default batch transaction
	Table         string `uri:"table" required:"true"` // insert table name i.e., "schema.table_name"
	UseFileBuffer bool   `uri:"use-file-buffer"`       // directs the writer to use a file buffer instead of in-memory when writing final deduped records
}

type worker struct {
	*options

	iOpt    InfoOptions
	records int64
}

func (o *options) newWorker(info string) task.Worker {
	return &worker{}
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	return task.Completed("completed")
}
