package file

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"

	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
)

var (
	defaultSep     = ","
	defaultDateFmt = time.RFC3339 // "2006-01-02T15:04:05Z07:00"
)

func NewWriteByHour(destTmpl string, opt *Options) *WriteByHour {
	if opt == nil {
		opt = NewOptions()
	}

	return &WriteByHour{
		opt:      opt,
		destTmpl: destTmpl,
		writers:  make(map[string]Writer),
	}
}

// WriteByHour writes to hourly files based on the
// extracted time.Time from the WriteLine bytes.
type WriteByHour struct {
	opt *Options // file buffer options

	// write file destination template
	// Example:
	// s3://bucket/base/dir/{YYYY}/{MM}/{DD}/{HH}/{TS}.txt
	destTmpl string

	// writers map key is the destination file path (parsed destTmpl)
	writers map[string]Writer
	lineCnt stat.Stats // just for keeping track of total line count.
	done    bool       // set to true if either Close or Abort are called. Prevents subsequent writes.
	mu      sync.RWMutex
	wg      sync.WaitGroup
}

// WriteLine will attempt to extract a date from the
// line bytes and write to a destination file path
// from the parsed destTmpl file template.
//
// An error is returned if there is a problem writing the line
// or if there is a problem extracting the date.
//
// Write order is not guaranteed.
func (w *WriteByHour) WriteLine(ln []byte, t time.Time) (err error) {
	// generate path
	pth := tmpl.Parse(w.destTmpl, t)

	w.mu.Lock()
	defer w.mu.Unlock()

	// writer
	writer, found := w.writers[pth]
	if !found {
		// create
		var err error
		writer, err = NewWriter(pth, nil)
		if err != nil {
			return err
		}

		w.writers[pth] = writer
	}

	err = writer.WriteLine(ln)
	if err == nil {
		w.lineCnt.AddLine()
	}
	return err
}

// LineCnt will provide the totals number of
// lines written across all files.
func (w *WriteByHour) LineCnt() int64 {
	sts := w.lineCnt

	return sts.LineCnt
}

// Stats provides stats for all files.
func (w *WriteByHour) Stats() []stat.Stats {
	var stats []stat.Stats
	for _, writer := range w.writers {
		stats = append(stats, writer.Stats())
	}
	return stats
}

// Abort will abort on all open files. If there
// are multiple non-nil errors it will return
// one of them.
func (w *WriteByHour) Abort() error {
	var err error
	for _, writer := range w.writers {
		aErr := writer.Abort()
		if aErr != nil {
			err = aErr
		}
	}

	return err
}

// Close will close all open files. If there
// are multiple non-nil errors it will return
// one of them.
//
// All writers are closed simultaneously so if
// an error is returned it's possible that one
// or more writes succeeded. Therefore the result
// space could be mixed with successes and failures.
//
// To know which ones succeeded, check through
// all the file stats by calling Stats and look
// for non-empty Stats.Created values.
// For this reason it is recommended that records
// should be written to destination files in such
// a way that re-running sort from the same data source
// will replace an existing sorted file instead of
// creating a new one.
//
// Make sure writing is complete before calling Close.
func (w *WriteByHour) Close() error {
	var err error
	for _, writer := range w.writers {
		// if an error is found then abort
		// the remaining writers.
		if err != nil {
			writer.Abort()
			continue
		}
		cErr := writer.Close()
		if cErr != nil {
			err = cErr
		}
	}

	return err
}

// CloseWContext is just like close but accepts a context.
// ctx.Done is checked before starting each file close.
//
// Returns an error with body "interrupted" if prematurely
// shutdown by ctx.
func (w *WriteByHour) CloseWithContext(ctx context.Context) error {
	var err error
	for _, writer := range w.writers {
		// if an error is found then abort
		// the remaining writers.
		if err != nil {
			writer.Abort()
			continue
		}

		// context cancel
		if err = ctx.Err(); err != nil {
			err = errors.New("interrupted")
			writer.Abort()
			continue
		}

		cErr := writer.Close()
		if cErr != nil {
			err = cErr
		}
	}

	return err
}

// DateExtractor defines a type that will parse raw
// bytes and attempt to extract a time.Time value.
//
// The underlying bytes should not be modified.
//
// If time.Time.IsZero() == true then a non-nil error
// should always be returned. Likewise if error != nil
// time.Time.IsZero() should always be true.
type DateExtractor func([]byte) (time.Time, error)

// CSVDateExtractor returns a DateExtractor for csv
// row date extraction.
//
// If negative field index is set to 0.
// sep and timeFmt
func CSVDateExtractor(sep, format string, fieldIndex int) DateExtractor {
	if sep == "" {
		sep = defaultSep
	}

	if format == "" {
		format = defaultDateFmt
	}

	if fieldIndex < 0 {
		fieldIndex = 0
	}

	extractor := &csvDateExtractor{
		separator:  sep,
		format:     format,
		fieldIndex: fieldIndex,
	}
	return extractor.ExtractDate
}

// csvDateExtractor implements ExtractDate which is of type DateExtractor.
type csvDateExtractor struct {
	separator  string
	format     string
	fieldIndex int // date field column index (0 indexed)
}

// ExtractDate is of type DateExtractor and expects
// b bytes to be csv row.
func (p *csvDateExtractor) ExtractDate(b []byte) (time.Time, error) {
	var t time.Time
	t.IsZero()
	s := strings.Split(string(b), p.separator)
	if len(s) <= p.fieldIndex {
		return t, fmt.Errorf("index %v not in '%v'", p.fieldIndex, string(b))
	}

	return time.Parse(p.format, s[p.fieldIndex])
}

// JSONDateExtractor returns a DateExtractor for json
// object date extraction.
func JSONDateExtractor(field, timeFmt string) DateExtractor {
	if timeFmt == "" {
		timeFmt = defaultDateFmt
	}

	extractor := &jsonDateExtractor{
		fieldName: field,
		format:    timeFmt,
	}

	return extractor.ExtractDate
}

// jsonDateExtractor implements ExtractDate which is of type DateExtractor.
type jsonDateExtractor struct {
	fieldName string // json time field
	format    string // time field time format
}

// ExtractDate is of type DateExtractor and expects
// b bytes to be a single json object.
func (p *jsonDateExtractor) ExtractDate(b []byte) (time.Time, error) {
	var t time.Time
	s, err := jsonparser.GetString(b, p.fieldName)
	if err != nil {
		return t, fmt.Errorf(`field "%v" not in '%v'`, p.fieldName, string(b))
	}

	return time.Parse(p.format, s)
}
