package sortbyhour

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/pcelvng/task"
)

type Worker struct {
	In         string
	Type       string `desc:"Type of file to parse",default:"raw"`
	Path       string `desc:"Path to write files to"`
	Prefix     string `desc:"Name of file"`
	FileFormat string `desc:"Go time format for file path",default:"20060102T150000"`

	TimeFormat  string `desc:"format of time to parse",default:"2006-01-02T15:04:05Z07:00"`
	JsonTimeTag string `desc:"a raw field containing time"`
	TimeIndex   int    `desc:""`
	files       map[string]io.WriteCloser
}

// NewWorker creates a new sortbyhour worker and implements the
func NewWorker(info string, _ context.Context) task.Worker {
	data, _ := parseInfo(info)
	return &Worker{
		In:         data["in"],
		Path:       data["path"],
		Prefix:     data["prefix"],
		TimeFormat: data["timeformat"],
		FileFormat: "20060102T150405",
		files:      make(map[string]io.WriteCloser),
	}

	//todo parse jsonTimeTag and TimeIndex
}

// shouldn't DoTask be passed the context?
func (w *Worker) DoTask() (task.Result, string) {
	f, err := os.Open(w.In)
	if err != nil {
		return task.ErrResult, fmt.Sprintf("Probably with path %s. %v", w.In, err.Error())
	}
	reader := bufio.NewScanner(f)
	var fn func(b []byte) (time.Time, error)
	switch w.Type {
	case "raw":
		fn = w.processJson
	case "tab":
		fn = w.processTab
	default:
		return task.ErrResult, fmt.Sprintf("unsupported type: %s", w.Type)
	}

	for reader.Scan() {
		b := reader.Bytes()
		t, err := fn(b)
		if err != nil {
			return task.ErrResult, err.Error()
		}
		if err := w.Write(t, b); err != nil {
			return task.ErrResult, err.Error()
		}
	}
	w.Close()
	return task.CompleteResult, "successful"
}

func (w *Worker) processJson(b []byte) (time.Time, error) {
	var t time.Time
	d, err := jsonparser.GetString(b, w.JsonTimeTag)
	if err != nil {
		return t, fmt.Errorf("JSON: cannot find %b in %b", w.JsonTimeTag, b)
	}
	return time.Parse(time.RFC3339, d)
}

func (w *Worker) processTab(b []byte) (time.Time, error) {
	var t time.Time
	s := strings.Split(string(b), "|")

	if len(s) < w.TimeIndex {
		return t, fmt.Errorf("%d field not found, length is %d", w.TimeIndex, len(s))
	}
	fmt.Println(w.TimeFormat, s[w.TimeIndex])
	return time.Parse(w.TimeFormat, s[w.TimeIndex])
}

func (w *Worker) Write(t time.Time, b []byte) (err error) {
	key := t.Format("2006-01-02T15")
	f, found := w.files[key]
	if !found {
		name := fmt.Sprintf("%s/%s_%s.log.gz", w.Path, w.Prefix, t.Format(w.FileFormat))
		f, err = os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}

		f, err = gzip.NewWriterLevel(f, gzip.BestSpeed)
		if err != nil {
			return err
		}
		w.files[key] = f
	}

	_, err = f.Write(append(b, '\n'))
	return err
}

func (w *Worker) Close() error {
	for _, f := range w.files {
		f.Close()
	}
	return nil
}

func parseInfo(info string) (map[string]string, error) {
	data := make(map[string]string)
	d := strings.Split(info, "|")
	for _, v := range d {
		s := strings.Split(v, "=")
		if len(s) == 2 {
			data[s[0]] = s[1]
		} else if len(s) == 1 {
			data[s[0]] = "true"
		} else {
			return data, fmt.Errorf("could not parse '%s'", v)
		}
	}
	return data, nil
}
