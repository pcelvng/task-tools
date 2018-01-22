package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/jbsmith7741/go-tools/uri2struct"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
)

func newInfoOptions(info string) (*infoOptions, error) {
	iOpt := &infoOptions{}
	err := uri2struct.Convert(iOpt, info)
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
		// date-field-index required
		if i.DateField == "" {
			return errors.New(`date-field-index required`)
		}
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
	iOpt, err := newInfoOptions(info)
	if iOpt == nil {
		iOpt = &infoOptions{} // in case of err
	}

	// validate
	vErr := iOpt.validate()
	if vErr != nil && err == nil {
		err = vErr // don't override existing err
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
	r, rErr := file.NewReader(iOpt.SrcPath, fOpt)
	if rErr != nil && err == nil {
		err = rErr // don't override existing err
	}

	// writer
	w := file.NewWriteByHour(iOpt.DestTemplate, fOpt)

	return &Worker{
		iOpt:        *iOpt,
		fOpt:        *fOpt,
		r:           r,
		w:           w,
		err:         err,
		extractDate: extractor,
	}
}

type Worker struct {
	iOpt         infoOptions
	fOpt         file.Options
	r            file.StatsReadCloser
	w            *file.WriteByHour
	err          error // initialization error
	extractDate  file.DateExtractor
	discardedCnt int64 // number of records discarded
}

func (wkr *Worker) DoTask(ctx context.Context) (task.Result, string) {
	// report initialization error
	if wkr.err != nil {
		return task.ErrResult, fmt.Sprint(wkr.err.Error())
	}

	// read/write loop
	done := false
	for !done {
		select {
		case <-ctx.Done():
			return task.ErrResult, "task interrupted"
		default:
			// read line
			ln, rErr := wkr.r.ReadLine()
			if rErr != nil && rErr != io.EOF {
				wkr.r.Close()
				wkr.w.Abort() // cleanup writes to this point

				msg := fmt.Sprintf("issue at line %v: %v", wkr.r.Stats().LineCnt+1, rErr.Error())
				return task.ErrResult, msg
			}

			// write line
			err := wkr.writeLine(ln)
			if err != nil {
				wkr.r.Close()
				wkr.w.Abort() // cleanup writes to this point

				msg := fmt.Sprintf("issue at line %v: %v", wkr.r.Stats().LineCnt, err.Error())
				return task.ErrResult, msg
			}

			// EOF
			if rErr == io.EOF {
				done = true
			}
		}
	}

	// close
	wkr.r.Close()
	err := wkr.w.Close()
	if err != nil {
		return task.ErrResult, fmt.Sprint(wkr.err.Error())
	}

	// publish files stats
	allSts := wkr.w.Stats()
	for _, sts := range allSts {
		producer.Send(appOpt.FileTopic, sts.JSONBytes())
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

// writeLine
// -extracts date from ln
// -handles discarding
// -does WriteByHour write
func (wkr *Worker) writeLine(ln []byte) error {
	if len(ln) > 0 {
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
			log.Printf("parse: '%v' at line %v for '%v'", err.Error(), wkr.r.Stats().LineCnt, string(ln))
		} else {
			return err
		}
	}

	return wkr.w.WriteLine(ln, t)
}
