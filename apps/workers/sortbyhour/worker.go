package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jbsmith7741/go-tools/uri2struct"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
)

func newInfoOptions(info string) (*infoOptions, error) {
	iOpt := &infoOptions{}
	err := uri2struct.Unmarshal(iOpt, info)
	return iOpt, err
}

// infoOptions contains the parsed info values
// of a task.
type infoOptions struct {
	SrcPath        string `uri:"origin"`           // source file path
	RecordType     string `uri:"record-type"`      // required; type of record to parse (options: json or csv)
	DateField      string `uri:"date-field"`       // json date field
	DateFieldIndex int    `uri:"date-field-index"` // csv date field index
	DateFormat     string `uri:"date-format"`      // expected date format (go time.Time format)
	CSVSep         string `uri:"sep"`              // csv separator (default=",")
	DestTemplate   string `uri:"dest-template"`    // template for destination files
	Discard        bool   `uri:"discard"`          // discard the record on error or end the task with an error
	UseFileBuffer  bool   `uri:"use-file-buffer"`  // directs the writer to use a file buffer instead of in-memory
}

// validate populated info options
func (i *infoOptions) validate() error {
	// record type validation
	switch i.RecordType {
	case "json":
		// date-field required
		if i.DateField == "" {
			return errors.New(`date-field required`)
		}
	case "csv":
	default:
		return errors.New(`record-type must be "csv" or "json"`)
	}

	// dest-template required
	if i.DestTemplate == "" {
		return errors.New(`dest-template required`)
	}

	return nil
}

func MakeWorker(info string) task.Worker {
	iOpt, _ := newInfoOptions(info)

	// validate
	err := iOpt.validate()
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	// date extractor
	var extractor file.DateExtractor
	switch iOpt.RecordType {
	case "json":
		extractor = file.JSONDateExtractor(
			iOpt.DateField,
			iOpt.DateFormat,
		)
	case "csv":
		extractor = file.CSVDateExtractor(
			iOpt.CSVSep,
			iOpt.DateFormat,
			iOpt.DateFieldIndex,
		)
	}

	// file opts
	fOpt := file.NewOptions()
	fOpt.UseFileBuf = iOpt.UseFileBuffer
	fOpt.FileBufDir = appOpt.FileBufferDir
	fOpt.FileBufPrefix = fileBufPrefix
	fOpt.AWSAccessKey = appOpt.AWSAccessKey
	fOpt.AWSSecretKey = appOpt.AWSSecretKey

	// reader
	r, err := file.NewReader(iOpt.SrcPath, fOpt)
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	// destination template
	destTempl := parseTmpl(iOpt.SrcPath, iOpt.DestTemplate)

	// writer
	w := file.NewWriteByHour(destTempl, fOpt)

	return &Worker{
		iOpt:        *iOpt,
		fOpt:        *fOpt,
		r:           r,
		w:           w,
		extractDate: extractor,
	}
}

type Worker struct {
	iOpt         infoOptions
	fOpt         file.Options
	r            file.Reader
	w            *file.WriteByHour
	extractDate  file.DateExtractor
	discardedCnt int64 // number of records discarded
}

func (wkr *Worker) DoTask(ctx context.Context) (task.Result, string) {
	// read/write loop
	for ctx.Err() == nil {
		ln, err := wkr.r.ReadLine()
		if err != nil && err != io.EOF {
			return wkr.abort(fmt.Sprintf("issue at line %v: %v", wkr.r.Stats().LineCnt+1, err.Error()))
		}

		wErr := wkr.writeLine(ln)
		if wErr != nil {
			return wkr.abort(fmt.Sprintf("issue at line %v: %v", wkr.r.Stats().LineCnt, wErr.Error()))
		}

		if err == io.EOF {
			break
		}
	}

	if task.IsDone(ctx) {
		wkr.abort("")
		return task.Interrupted()
	}
	return wkr.done(ctx)
}

// writeLine
// -extracts date from ln
// -handles discarding
// -does WriteByHour write
func (wkr *Worker) writeLine(ln []byte) error {
	if len(ln) == 0 {
		return nil
	}

	// extract date
	t, err := wkr.extractDate(ln)

	// handle err
	// Discard == true: continue processing
	// Discard == false: halt processing, error
	if err != nil {
		if wkr.iOpt.Discard {
			wkr.discardedCnt += 1
			// TODO: add with central logging
			//log.Printf("parse: '%v' at line %v for '%v'", err.Error(), wkr.r.Stats().LineCnt, string(ln))
			return nil
		} else {
			return err
		}
	}

	return wkr.w.WriteLine(ln, t)
}

// abort will abort processing by closing the
// reading and then cleaning up written records.
func (wkr *Worker) abort(msg string) (task.Result, string) {
	wkr.r.Close()
	wkr.w.Abort() // cleanup writes to this point

	return task.ErrResult, msg
}

// done assumes writing is done; will finalize
// writes and return a task response. Will also
// handle sending created files messages on the
// producer.
func (wkr *Worker) done(ctx context.Context) (task.Result, string) {
	// close
	wkr.r.Close()
	err := wkr.w.CloseWithContext(ctx)
	if err != nil {
		return task.ErrResult, fmt.Sprint(err.Error())
	}

	// publish files stats
	allSts := wkr.w.Stats()
	for _, sts := range allSts {
		if sts.Size > 0 { // only successful files
			producer.Send(appOpt.FileTopic, sts.JSONBytes())
		}
	}

	// msg
	var msg string
	if wkr.iOpt.Discard {
		msg = fmt.Sprintf("wrote %v lines over %v files (%v discarded)", wkr.w.LineCnt(), len(allSts), wkr.discardedCnt)
	} else {
		msg = fmt.Sprintf("wrote %v lines over %v files", wkr.w.LineCnt(), len(allSts))
	}

	return task.CompleteResult, msg
}

// parseTmpl is a one-time tmpl parsing that supports the
// following template tags:
// - {SRC_FILE} string value of the source file. Not the full path. Just the file name, including extensions.
// - {SRC_TS}   source file timestamp (if available) in following format: 20060102T150405
//
// If templ contains any of the supported template tokens but that token
// is unable to be populated from srcPth then an error is returned. The existence
// of a non-nil error will return parsedTmpl as the unmodified input tmpl value.
//
// If templ does not contain any of the supported tokens, then parsedTmpl is
// returned as the unmodified tmpl value and err == nil.
//
// If the source file full path was:
// s3://bucket/path/2017/02/03/16/file-20070203T160101.json.gz
//
// Then the value of {SRC_FILE} would be:
// file-20070203T160101.json.gz
//
// And the value of {SRC_TS} would be:
// 20070203T160101
func parseTmpl(srcPth, tmpl string) string {
	_, srcFile := filepath.Split(srcPth)

	re := regexp.MustCompile(`[0-9]{8}T[0-9]{6}`)
	srcTS := re.FindString(srcFile)

	// {SRC_FILE}
	if srcFile != "" {
		tmpl = strings.Replace(tmpl, "{SRC_FILE}", srcFile, -1)
	}

	// {SRC_TS}
	if srcTS != "" {
		tmpl = strings.Replace(tmpl, "{SRC_TS}", srcTS, -1)
	}

	return tmpl
}
