package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
)

func newInfoOptions(info string) (*infoOptions, error) {
	iOpt := &infoOptions{}
	err := uri.Unmarshal(info, iOpt)
	return iOpt, err
}

// infoOptions contains the parsed info values
// of a task.
type infoOptions struct {
	SrcPath       string `uri:"origin"`          // source file path
	DateField     string `uri:"date-field"`      // json date field, unless sep is provided then must be integer and expecting csv style format records.
	DateFormat    string `uri:"date-format"`     // expected date format (go time.Time format)
	Sep           string `uri:"sep"`             // csv separator - must be provided to indicate csv style records
	DestTemplate  string `uri:"dest-template"`   // template for destination files
	Discard       bool   `uri:"discard"`         // discard the record on error or end the task with an error
	UseFileBuffer bool   `uri:"use-file-buffer"` // directs the writer to use a file buffer instead of in-memory
}

// validate populated info options
func (i *infoOptions) validate() error {
	// date-field required
	if len(i.DateField) == 0 {
		return errors.New(`date-field required`)
	}

	// date-field index value if sep is present
	if len(i.Sep) > 0 {
		// attempt to convert DateField to int
		var err error
		_, err = strconv.Atoi(i.DateField)
		if err != nil {
			return errors.New(`date-field must be an integer when using a csv field separator`)
		}
	}

	// dest-template required
	if i.DestTemplate == "" {
		return errors.New(`dest-template required`)
	}

	return nil
}

func newWorker(info string) task.Worker {
	iOpt, _ := newInfoOptions(info)

	// validate
	err := iOpt.validate()
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	// date extractor
	var extractor file.DateExtractor
	if len(iOpt.Sep) > 0 { // if using sep then record type is csv
		dateIndex, _ := strconv.Atoi(iOpt.DateField) // should not return error since validation already figured that out.

		extractor = file.CSVDateExtractor(
			iOpt.Sep,
			iOpt.DateFormat,
			dateIndex,
		)
	} else { // no sep then assume json
		extractor = file.JSONDateExtractor(
			iOpt.DateField,
			iOpt.DateFormat,
		)
	}

	// file opts
	wfOpt := file.NewOptions()
	wfOpt.UseFileBuf = iOpt.UseFileBuffer || fOpt.UseFileBuf
	wfOpt.FileBufDir = fOpt.FileBufDir
	wfOpt.FileBufPrefix = fOpt.FileBufPrefix
	wfOpt.AccessKey = fOpt.AccessKey
	wfOpt.SecretKey = fOpt.SecretKey

	// all paths (if pth is directory)
	fSts, _ := file.List(iOpt.SrcPath, wfOpt)

	// path not directory - assume just one file
	if len(fSts) == 0 {
		sts := stat.New()
		sts.Path = iOpt.SrcPath
		fSts = append(fSts, sts)
	}

	// reader(s)
	stsRdrs := make([]*statsReader, 0)
	for _, sts := range fSts {
		if sts.IsDir {
			continue
		}
		sr := &statsReader{sts: &sts}
		sr.r, err = file.NewReader(sts.Path, wfOpt)
		if err != nil {
			return task.InvalidWorker(err.Error())
		}
		stsRdrs = append(stsRdrs, sr)
	}

	// destination template
	destTempl := parseTmpl(iOpt.SrcPath, iOpt.DestTemplate)

	// writer
	w := file.NewWriteByHour(destTempl, wfOpt)

	return &worker{
		iOpt:        *iOpt,
		fOpt:        *wfOpt,
		stsRdrs:     stsRdrs,
		w:           w,
		extractDate: extractor,
	}
}

type statsReader struct {
	sts *stat.Stats
	r   file.Reader
}

type worker struct {
	iOpt         infoOptions
	fOpt         file.Options
	stsRdrs      []*statsReader
	w            *file.WriteByHour
	extractDate  file.DateExtractor
	discardedCnt int64 // number of records discarded
}

func (wkr *worker) DoTask(ctx context.Context) (task.Result, string) {
	// read/write loop
	for _, rdr := range wkr.stsRdrs { // loop through all readers
		sts := rdr.sts
		r := rdr.r

		for ctx.Err() == nil {
			ln, err := r.ReadLine()
			if err != nil && err != io.EOF {
				return wkr.abort(fmt.Sprintf("issue at line %v: %v (%v)", r.Stats().LineCnt+1, err.Error(), sts.Path))
			}

			wErr := wkr.writeLine(ln)
			if wErr != nil {
				return wkr.abort(fmt.Sprintf("issue at line %v: %v (%v)", r.Stats().LineCnt, wErr.Error(), sts.Path))
			}

			if err == io.EOF {
				break
			}
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
func (wkr *worker) writeLine(ln []byte) error {
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
func (wkr *worker) abort(msg string) (task.Result, string) {
	for _, rdr := range wkr.stsRdrs {
		rdr.r.Close()
	}
	wkr.w.Abort() // cleanup writes to this point

	return task.ErrResult, msg
}

// done assumes writing is done; will finalize
// writes and return a task response. Will also
// handle sending created files messages on the
// producer.
func (wkr *worker) done(ctx context.Context) (task.Result, string) {
	// close
	for _, rdr := range wkr.stsRdrs {
		rdr.r.Close()
	}
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
//              If reading from all files in a directory then SRC_TS is derived from the path
//              slug. So a path with /2018/02/03/04/ would show 20180203T040000 and
//              a path with /2018/02/03/ (but no hour) would show 20180203T000000
//              a path with /2018/02/ (but no day) would show 20180200T000000. Only having
//              a year value in the path time slug is not supported.
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
	srcDir, srcFile := filepath.Split(srcPth)

	// filename regex
	re := regexp.MustCompile(`[0-9]{8}T[0-9]{6}`)
	srcTS := re.FindString(srcFile)

	// hour slug regex
	hSlugRe := regexp.MustCompile(`[0-9]{4}\/[0-9]{2}\/[0-9]{2}\/[0-9]{2}`)
	hSrcTS := hSlugRe.FindString(srcDir)

	// day slug regex
	dSlugRe := regexp.MustCompile(`[0-9]{4}\/[0-9]{2}\/[0-9]{2}`)
	dSrcTS := dSlugRe.FindString(srcDir)

	// month slug regex
	mSlugRe := regexp.MustCompile(`[0-9]{4}\/[0-9]{2}`)
	mSrcTS := mSlugRe.FindString(srcDir)

	// {SRC_FILE}
	if srcFile != "" {
		tmpl = strings.Replace(tmpl, "{SRC_FILE}", srcFile, -1)
	}

	// {SRC_TS}
	tsFmt := "20060102T150405" // output format
	if srcTS != "" {
		// src ts in filename
		tmpl = strings.Replace(tmpl, "{SRC_TS}", srcTS, -1)
	} else if hSrcTS != "" {
		// src ts in hour slug
		hFmt := "2006/01/02/15"
		t, _ := time.Parse(hFmt, hSrcTS)
		if !t.IsZero() {
			tmpl = strings.Replace(tmpl, "{SRC_TS}", t.Format(tsFmt), -1)
		}
	} else if dSrcTS != "" {
		// src ts in day slug
		dFmt := "2006/01/02"
		t, _ := time.Parse(dFmt, dSrcTS)
		if !t.IsZero() {
			tmpl = strings.Replace(tmpl, "{SRC_TS}", t.Format(tsFmt), -1)
		}
	} else if mSrcTS != "" {
		// src ts in month slug
		mFmt := "2006/01"
		t, _ := time.Parse(mFmt, mSrcTS)
		if !t.IsZero() {
			tmpl = strings.Replace(tmpl, "{SRC_TS}", t.Format(tsFmt), -1)
		}
	}

	return tmpl
}
