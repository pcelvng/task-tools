package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/dedup"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
)

func newInfoOptions(info string) (*infoOptions, error) {
	iOpt := &infoOptions{}
	err := uri.Unmarshal(info, iOpt)
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
			if err != nil && !regexIndexRange.MatchString(indexField) {
				return fmt.Errorf("invalid field %v for csv file", indexField)
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
	wfOpt := file.NewOptions()
	wfOpt.UseFileBuf = iOpt.UseFileBuffer
	wfOpt.FileBufDir = fOpt.FileBufDir
	wfOpt.FileBufPrefix = fOpt.FileBufPrefix
	wfOpt.AccessKey = fOpt.AccessKey
	wfOpt.SecretKey = fOpt.SecretKey

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
	stsFiles := make(StatsFiles, len(fSts))

	for i, sts := range fSts {
		stsFiles[i].Stats = sts
		stsFiles[i].pthTime = tmpl.PathTime(sts.Path)
	}

	// sort readers (oldest to newest)
	sort.Sort(stsFiles) // implements sort interface

	// parse destination template
	destPth := parseTmpl(iOpt.SrcPath, iOpt.DestTemplate)

	// writer
	w, err := file.NewWriter(destPth, fOpt)
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	// csv index fields
	indexFields := make([]int, 0, len(iOpt.Fields))
	if len(iOpt.Sep) > 0 {
		for _, indexField := range iOpt.Fields {
			if v, err := strconv.Atoi(indexField); err == nil {
				indexFields = append(indexFields, v)
			} else if regexIndexRange.MatchString(indexField) {
				// expand out integer ranges
				d := strings.Split(indexField, "-")
				start, _ := strconv.Atoi(d[0])
				end, _ := strconv.Atoi(d[1])
				for i := start; i <= end; i++ {
					indexFields = append(indexFields, i)
				}
			} else {
				return task.InvalidWorker("invalid index field %s", indexField)
			}
		}
	}

	return &worker{
		iOpt:     *iOpt,
		stsFiles: stsFiles,
		dedup:    dedup.New(),
		w:        w,

		indexFields: indexFields,
	}
}

// regexIndexRange checks if a string is a range of integers. ex 1-10
var regexIndexRange = regexp.MustCompile(`^[0-9]*[-][0-9]*$`)

type worker struct {
	iOpt         infoOptions
	stsFiles     []StatsFile
	linesWritten int64
	files        []string
	dedup        *dedup.Dedup
	w            file.Writer

	// csv
	indexFields []int // csv index fields (set during validation)
}

func (wkr *worker) DoTask(ctx context.Context) (task.Result, string) {
	// read/write loop
	for _, rdr := range wkr.stsFiles { // loop through all readers
		r, err := file.NewReader(rdr.Path, fOpt)
		if err != nil {
			return task.Failed(err)
		}
		scanner := file.NewScanner(r)
		for scanner.Scan() {
			if task.IsDone(ctx) {
				r.Close()
				return task.Interrupted()
			}

			wkr.addLine(scanner.Bytes())
		}
		r.Close()
		wkr.linesWritten += r.Stats().LineCnt
		if err := scanner.Err(); err != nil {
			return task.Failf("issue at line %v: %v (%v)", r.Stats().LineCnt+1, err.Error(), rdr.Path)
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
	wkr.w.Abort() // cleanup writes to this point

	return task.ErrResult, msg
}

// done assumes writing is done; will finalize
// writes and return a task response. Will also
// handle sending created files messages on the
// producer.
func (wkr *worker) done() (task.Result, string) {
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
	msg := fmt.Sprintf(`read %v lines from %v files and wrote %v lines`, wkr.linesWritten, len(wkr.stsFiles), wkr.w.Stats().LineCnt)

	return task.CompleteResult, msg
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

type StatsFile struct {
	stat.Stats
	pthTime time.Time // src path extracted time
}

type StatsFiles []StatsFile

func (d StatsFiles) Len() int {
	return len(d)
}

func (d StatsFiles) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

// Less will check if:
// i path time is less than j path time
// if i and j path times are equal then
// i created date will check if less then j created.
func (d StatsFiles) Less(i, j int) bool {
	// first by path src date
	if d[i].pthTime.Equal(d[j].pthTime) {
		// then by created date (if path src date is equal)
		return d[i].ParseCreated().Before(d[j].ParseCreated())
	}

	// is i pthTime before j pthTime?
	return d[i].pthTime.Before(d[j].pthTime)
}
