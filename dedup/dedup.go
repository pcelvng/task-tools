package dedup

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jbsmith7741/go-tools/uri2struct"
	"github.com/json-iterator/go"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
)

type Worker struct {
	Key       []string
	TimeField string
	Keep      string
	data      map[string]string
	ReadPath  string `uri:"origin"`
	WritePath string
	writer    file.Writer
	reader    file.Reader
}

type Config struct {
	file.Options
}

const (
	Newest = "newest"
	Oldest = "oldest"
	First  = "first"
	Last   = "last"
)

func (c *Config) NewWorker(info string) task.Worker {
	w := &Worker{
		Keep: Newest,
		data: make(map[string]string),
	}

	var err error
	if err := uri2struct.Unmarshal(w, info); err != nil {
		return task.InvalidWorker("Error parsing info: %s", err)
	}

	if w.writer, err = file.NewWriter(w.WritePath, &c.Options); err != nil {
		return task.InvalidWorker("invalid write path '%s'", w.WritePath)
	}

	if w.reader, err = file.NewReader(w.ReadPath, &c.Options); err != nil {
		return task.InvalidWorker("invalid read path '%s'", w.ReadPath)
	}
	return w
}

func (w *Worker) DoTask(ctx context.Context) (task.Result, string) {

	// read
	//reader := bufio.NewScanner(w.reader)

	for ln, err := w.reader.ReadLine(); err != io.EOF; ln, err = w.reader.ReadLine() {
		if err != nil {
			return task.Failed(err)
		}
		if task.IsDone(ctx) {
			return task.Interrupted()
		}
		if err := w.dedup(ln); err != nil {
			return task.Failed(err)
		}
	}
	w.reader.Close()

	// write
	defer w.writer.Close()
	for _, b := range w.data {
		if task.IsDone(ctx) {
			return task.Interrupted()
		}
		err := w.writer.WriteLine([]byte(b))
		if err != nil {
			return task.Failed(err)
		}
	}
	return task.Completed("Lines written: %d", w.writer.Stats().LineCnt)
}

func (w *Worker) dedup(b []byte) error {
	var key string
	for _, k := range w.Key {
		s := jsoniter.Get(b, k).ToString()
		key += s + "|"
	}
	key = strings.TrimRight(key, "|")

	if w.Keep == Last {
		w.data[key] = string(b)
		return nil
	}

	data, found := w.data[key]

	// always keep first occurrence
	if !found && w.Keep == First {
		w.data[key] = string(b)
		return nil
	}

	newTime, err := time.Parse(time.RFC3339, jsoniter.Get(b, w.TimeField).ToString())
	if err != nil {
		return fmt.Errorf("%s:%s is not a valid RFC3339 time", jsoniter.Get(b, w.TimeField).ToString(), w.TimeField)
	}

	if !found {
		w.data[key] = string(b)
		return nil
	}

	oldTime, _ := time.Parse(time.RFC3339, jsoniter.Get([]byte(data), w.TimeField).ToString())

	if (w.Keep == Newest && newTime.After(oldTime)) || (w.Keep == Oldest && newTime.Before(oldTime)) {
		w.data[key] = string(b)
	}
	return nil
}
