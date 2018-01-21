package main

import (
	"context"

	"github.com/jbsmith7741/go-tools/uri2struct"
	"github.com/pcelvng/task"
)



// infoOptions contains the parsed info values
// of a task.
type infoOptions struct {
	SrcPath        string `uri:"origin"`           // source file path
	DateField      string `uri:"date-field"`       // json date field
	DateFieldIndex int    `uri:"date-field-index"` // csv date field index
	DateFormat     string `uri:"date-format"`      // expected date format (go time.Time format)
	DestTemplate   string `uri:"dest-template"`    // template for destination files
	Discard        bool   `uri:"discard"`          // discard the record on error or end the task with an error
	UseFileBuffer  bool   `uri:"use-file-buffer"`  // directs the writer to use a file buffer instead of in-memory
}

func MakeWorker(info string) task.Worker {
	iOpt := &infoOptions{}
	err := uri2struct.Convert(iOpt, info)

	return &Worker{
		info: info,
		iOpt: iOpt,
		err:  err,
	}
}

type Worker struct {
	info string
	iOpt *infoOptions
	err  error
}

func (w *Worker) DoTask(ctx context.Context) (result task.Result, msg string) {
	// extract date
	//t, err := w.extractDate(ln)
	//if err != nil {
	//	return err
	//}
	return result, msg
}
