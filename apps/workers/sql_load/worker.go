package main

import (
	"context"
	"os"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
)

type InfoOptions struct {
	FilePath string `uri:"origin"`                // file path to load one file or a list of files in that path (not recursive)
	Strict   bool   `uri:"strict"`                // field names in json line must match field names in the table
	Copy     bool   `uri:"copy"`                  // use a copy statement, and not the default batch transaction
	Table    string `uri:"table" required:"true"` // insert table name i.e., "schema.table_name"
}

type worker struct {
	options

	Params InfoOptions

	flist   []stat.Stats // list of file(s)
	records int64        // inserted records
}

func (o *options) newWorker(info string) task.Worker {
	var err error

	w := &worker{
		options: *o,
		flist:   make([]stat.Stats, 0),
	}

	if err := uri.Unmarshal(info, &w.Params); err != nil {
		return task.InvalidWorker("params uri.unmarshal: %v", err)
	}

	f, err := os.Stat(w.Params.FilePath)
	if err != nil {
		return task.InvalidWorker("filepath os: %v", err)
	}

	switch mode := f.Mode(); {
	case mode.IsDir():
		w.flist, _ = file.List(w.Params.FilePath, w.fileOpts)
	case mode.IsRegular():
		s, _ := file.Stat(w.Params.FilePath, w.fileOpts)
		w.flist = append(w.flist, s.Clone())
	}
	if len(w.flist) == 0 {
		return task.InvalidWorker("no files found in path %s", w.Params.FilePath)
	}
	return w
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	return task.Completed("completed")
}
