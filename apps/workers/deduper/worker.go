package main

import (
	"context"
	"errors"
	"io"
	"strconv"

	"github.com/jbsmith7741/go-tools/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/dedup"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
)

func newInfoOptions(info string) (*infoOptions, error) {
	iOpt := &infoOptions{}
	err := uri.Unmarshal(iOpt, info)
	return iOpt, err
}

// infoOptions contains the parsed info values
// of a task.
type infoOptions struct {
	SrcPath       string   `uri:"origin"`          // source file path - can be a directory or single file
	DestTemplate  string   `uri:"dest-template"`   // template for destination files
	Fields        []string `uri:"fields"`          // json fields list, unless sep is provided, then expecting field index values
	Sep           string   `uri:"sep"`             // csv separator - must be provided if expecting csv style records
	UseFileBuffer bool     `uri:"use-file-buffer"` // directs the writer to use a file buffer instead of in-memory when writing final deduped records

	indexFields []int // csv index fields (set during validation)
}

// validate populated info options
func (i *infoOptions) validate() error {
	// date-field required
	if len(i.Fields) == 0 {
		return errors.New(`fields required`)
	}

	// fields must convert to index value ints if sep is present
	if len(i.Sep) > 0 {
		var err error
		i.indexFields = make([]int, len(i.Fields))

		for n, indexField := range i.Fields {
			i.indexFields[n], err = strconv.Atoi(indexField)
			if err != nil {
				return errors.New(`fields must be integers when using a csv field separator`)
			}
		}
	}

	// dest-template required
	if i.DestTemplate == "" {
		return errors.New(`dest-template required`)
	}

	return nil
}

type StatsReader struct {
	sts *stat.Stats
	r   file.Reader
}

func NewWorker(info string) task.Worker {
	// parse info
	iOpt, _ := newInfoOptions(info)

	// validate
	err := iOpt.validate()
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	// file opts
	fOpt := file.NewOptions()
	fOpt.UseFileBuf = iOpt.UseFileBuffer
	fOpt.FileBufDir = appOpt.FileBufferDir
	fOpt.FileBufPrefix = fileBufPrefix
	fOpt.AWSAccessKey = appOpt.AWSAccessKey
	fOpt.AWSSecretKey = appOpt.AWSSecretKey

	// all paths (if pth is directory)
	fSts, _ := file.List(iOpt.SrcPath, fOpt)

	// path not directory - assume just one file
	if len(fSts) == 0 {
		sts := stat.New()
		sts.Path = iOpt.SrcPath
		fSts = append(fSts, sts)
	}

	// reader(s)
	stsRdrs := make([]*StatsReader, 0)
	for _, sts := range fSts {
		sr := &StatsReader{sts: &sts}
		sr.r, err = file.NewReader(sts.Path, fOpt)
		if err != nil {
			return task.InvalidWorker(err.Error())
		}
		stsRdrs = append(stsRdrs, sr)
	}

	// deduper
	dedup := dedup.New()

	// writer
	w, err := file.NewWriter(iOpt.DestTemplate, fOpt)
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	return &Worker{
		iOpt:    *iOpt,
		stsRdrs: stsRdrs,
		dedup:   dedup,
	}
}

type Worker struct {
	iOpt    infoOptions
	stsRdrs []*StatsReader
	dedup   *dedup.Dedup
}

func (w *Worker) DoTask(ctx context.Context) (task.Result, string) {
	for ln, err := w.r.ReadLine(); err != io.EOF; ln, err = w.r.ReadLine() {
		if err != nil {
			return task.Failed(err)
		}
		if task.IsDone(ctx) {
			return task.Interrupted()
		}
		if err := w.dedup.WriteLine("key", ln); err != nil {
			return task.Failed(err)
		}
	}
	w.r.Close()

	// finish write
	w.dedup.Close()
	for _, b := range w.data {
		if task.IsDone(ctx) {
			return task.Interrupted()
		}
		err := w.writer.WriteLine([]byte(b))
		if err != nil {
			return task.Failed(err)
		}
	}
	return task.Completed("lines written: %d", w.writer.Stats().LineCnt)
}
