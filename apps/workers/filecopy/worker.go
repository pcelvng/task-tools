package main

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"strings"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pkg/errors"
	"gopkg.in/jbsmith7741/uri.v0"
)

func newInfoOptions(info string) (*infoOptions, error) {
	iOpt := &infoOptions{}
	err := uri.Unmarshal(info, iOpt)
	return iOpt, err
}

// infoOptions contains the parsed info values
// of a task.
type infoOptions struct {
	SrcPath       string `uri:"origin"`          // source file path - can be a directory or single file
	DestTemplate  string `uri:"dest-template"`   // template for destination files
	UseFileBuffer bool   `uri:"use-file-buffer"` // directs the writer to use a file buffer instead of in-memory when writing final deduped records
}

type worker struct {
	iOpt         infoOptions
	linesWritten int64
	writer       file.Writer
	reader       file.Reader
	fileTopic    string
	// csv
	indexFields []int // csv index fields (set during validation)
}

// validate populated info options
func (i *infoOptions) validate() error {
	// dest-template required
	if i.DestTemplate == "" {
		return errors.New(`dest-template required`)
	}

	if i.SrcPath == "" {
		return errors.New(`src-path required`)
	}

	return nil
}

func (c *options) newWorker(info string) task.Worker {
	// parse info
	iOpt, err := newInfoOptions(info)
	if err != nil {
		return task.InvalidWorker("uri options: %v", err)
	}

	// validate
	err = iOpt.validate()
	if err != nil {
		return task.InvalidWorker(err.Error())
	}
	fileDay := tmpl.PathTime(iOpt.SrcPath)
	readPath := tmpl.Parse(iOpt.SrcPath, fileDay)
	writePath := parseTmpl(readPath, iOpt.DestTemplate)
	w := &worker{fileTopic: c.FileTopic}

	// create file writer
	w.writer, err = file.NewWriter(writePath, c.WriteOptions)
	if err != nil {
		return task.InvalidWorker("writer: cannot initialize writer %s", writePath)
	}

	// create file reader
	w.reader, err = file.NewReader(iOpt.SrcPath, c.ReadOptions)
	if err != nil {
		return task.InvalidWorker("invalid reader info path: %v", err)
	}

	return w
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	if ctx.Err() != nil {
		return task.Interrupted()
	}
	// copy the file from the reader to the writer
	_, err := io.Copy(w.writer, w.reader)
	w.reader.Close()
	if err != nil {
		return task.Failed(errors.Wrap(err, "io: copy"))
	}

	err = w.writer.Close()
	if err != nil {
		return task.Failed(err)
	}

	stats := w.writer.Stats()
	if err := producer.Send(w.fileTopic, stats.JSONBytes()); err != nil {
		log.Printf("could not publish to %s", w.fileTopic)
	}

	return task.Completed("Completed")
}

// parseTmpl is a one-time tmpl parsing that supports the
// following template tags:
// - {SRC_FILE} string value of the source file. Not the full path. Just the file name, including extensions.
//
// note that all template tags found from running tmpl.Parse() where the time passed in
// is the value of the discovered source ts.
func parseTmpl(srcPth, destTmpl string) string {
	_, srcFile := filepath.Split(srcPth)

	// {SRC_FILE}
	if srcFile != "" {
		destTmpl = strings.Replace(destTmpl, "{SRC_FILE}", srcFile, -1)
	}

	t := tmpl.PathTime(srcPth)
	return tmpl.Parse(destTmpl, t)
}
