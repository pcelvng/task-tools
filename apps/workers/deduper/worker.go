package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jbsmith7741/go-tools/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/dedup"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
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

		for _, indexField := range i.Fields {
			_, err = strconv.Atoi(indexField)
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

func newWorker(info string) task.Worker {
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
		// token sts for one src file
		sts := stat.New()
		sts.Path = iOpt.SrcPath
		fSts = append(fSts, sts)
	}

	// reader(s)
	stsRdrs := make(StatsReaders, len(fSts))
	for i, sts := range fSts {
		// reader
		r, err := file.NewReader(sts.Path, fOpt)
		if err != nil {
			return task.InvalidWorker(err.Error())
		}

		// stats reader
		stsRdrs[i] = &StatsReader{
			sts:     sts,
			pthTime: tmpl.PathTime(sts.Path),
			r:       r,
		}
	}

	// sort readers (oldest to newest)
	sort.Sort(stsRdrs) // implements sort interface

	// deduper
	dedup := dedup.New()

	// parse destination template
	destPth := parseTmpl(iOpt.SrcPath, iOpt.DestTemplate)

	// writer
	w, err := file.NewWriter(destPth, fOpt)
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	// csv index fields
	indexFields := make([]int, len(iOpt.Fields))
	if len(iOpt.Sep) > 0 {
		for n, indexField := range iOpt.Fields {
			indexFields[n], _ = strconv.Atoi(indexField)
		}
	}

	return &worker{
		iOpt:    *iOpt,
		stsRdrs: stsRdrs,
		dedup:   dedup,
		w:       w,

		indexFields: indexFields,
	}
}

type worker struct {
	iOpt    infoOptions
	stsRdrs []*StatsReader
	dedup   *dedup.Dedup
	w       file.Writer

	// csv
	indexFields []int // csv index fields (set during validation)
}

func (wkr *worker) DoTask(ctx context.Context) (task.Result, string) {
	// read/write loop
	for _, rdr := range wkr.stsRdrs { // loop through all readers
		sts := rdr.sts
		r := rdr.r

		for {
			if ctx.Err() != nil {
				wkr.abort("")
				return task.Interrupted()
			}

			ln, err := r.ReadLine()
			if err != nil && err != io.EOF {
				return wkr.abort(fmt.Sprintf("issue at line %v: %v (%v)", r.Stats().LineCnt+1, err.Error(), sts.Path))
			}

			wkr.addLine(ln)

			if err == io.EOF {
				break
			}
		}
	}

	// write deduped records
	for _, ln := range wkr.dedup.Lines() {
		select {
		case <-ctx.Done():
			wkr.abort("")
			return task.Interrupted()
		default:
			wkr.w.WriteLine(ln)
		}
	}

	return wkr.done()
}

// addLine
// -extracts key from ln
// -adds line and key to deduper
func (wkr *worker) addLine(ln []byte) {
	if len(ln) == 0 {
		return
	}

	// make key
	var key string
	if len(wkr.iOpt.Sep) > 0 {
		key = dedup.KeyFromCSV(ln, wkr.indexFields, wkr.iOpt.Sep)
	} else {
		key = dedup.KeyFromJSON(ln, wkr.iOpt.Fields)
	}

	// add
	wkr.dedup.Add(key, ln)
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
func (wkr *worker) done() (task.Result, string) {
	// close
	for _, rdr := range wkr.stsRdrs {
		rdr.r.Close()
	}
	err := wkr.w.Close()
	if err != nil {
		return task.ErrResult, fmt.Sprint(err.Error())
	}

	// publish files stats
	sts := wkr.w.Stats()
	if sts.Size > 0 { // only successful files
		producer.Send(appOpt.FileTopic, sts.JSONBytes())
	}

	// msg
	msg := fmt.Sprintf(`read %v lines from %v files and wrote %v lines`, wkr.linesRead(), len(wkr.stsRdrs), wkr.w.Stats().LineCnt)

	return task.CompleteResult, msg
}

// linesRead returns total lines read across all files read.
func (wkr *worker) linesRead() (lnCnt int64) {
	for _, rSts := range wkr.stsRdrs {
		lnCnt += rSts.r.Stats().LineCnt
	}

	return lnCnt
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

type StatsReader struct {
	sts     stat.Stats
	pthTime time.Time // src path extracted time
	r       file.Reader
}

type StatsReaders []*StatsReader

func (d StatsReaders) Len() int {
	return len(d)
}

func (d StatsReaders) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

// Less will check if:
// i path time is less than j path time
// if i and j path times are equal then
// i created date will check if less then j created.
func (d StatsReaders) Less(i, j int) bool {
	// first by path src date
	if d[i].pthTime.Equal(d[j].pthTime) {
		// then by created date (if path src date is equal)
		return d[i].sts.ParseCreated().Before(d[j].sts.ParseCreated())
	}

	// is i pthTime before j pthTime?
	return d[i].pthTime.Before(d[j].pthTime)
}
