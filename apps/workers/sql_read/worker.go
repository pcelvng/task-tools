package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/dustin/go-humanize"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jbsmith7741/uri"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
)

type worker struct {
	task.Meta

	db     *sqlx.DB
	writer file.Writer

	Fields FieldMap
	Query  string
}

type FieldMap map[string]string

func (o *options) NewWorker(info string) task.Worker {
	// unmarshal info string
	iOpts := struct {
		Table       string            `uri:"table"`
		QueryFile   string            `uri:"origin"` // path to query file
		Fields      map[string]string `uri:"field"`
		Destination string            `uri:"dest" required:"true"`
	}{}
	if err := uri.Unmarshal(info, &iOpts); err != nil {
		return task.InvalidWorker(err.Error())
	}

	var query string
	// get query
	if len(iOpts.Fields) > 0 {
		if s := strings.Split(iOpts.Table, "."); len(s) != 2 {
			return task.InvalidWorker("invalid table %s (schema.table)", iOpts.Table)
		}
		var cols string
		for k := range iOpts.Fields {
			cols += k + ", "
		}
		cols = strings.TrimRight(cols, ", ")
		query = fmt.Sprintf("select %s from %s", cols, iOpts.Table)
	}

	if iOpts.QueryFile != "" {
		r, err := file.NewReader(iOpts.QueryFile, o.FOpts)
		if err != nil {
			return task.InvalidWorker(err.Error())
		}
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return task.InvalidWorker(err.Error())
		}
		query = string(b)
	}

	if query == "" {
		return task.InvalidWorker("query path or field params required")
	}

	w, err := file.NewWriter(iOpts.Destination, o.FOpts)
	if err != nil {
		return task.InvalidWorker("writer: %s", err)
	}

	return &worker{
		Meta:   task.NewMeta(),
		db:     o.db,
		Fields: iOpts.Fields,
		Query:  query,
		writer: w,
	}
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	// pull Data from mysql database
	rows, err := w.db.QueryxContext(ctx, w.Query)
	if err != nil {
		return task.Failed(err)
	}
	for rows.Next() {
		if task.IsDone(ctx) {
			w.writer.Abort()
			return task.Interrupted()
		}
		row := make(map[string]interface{})
		if err := rows.MapScan(row); err != nil {
			return task.Failf("mapscan %s", err)
		}

		r := w.Fields.convertRow(row)
		b, err := jsoniter.Marshal(r)
		if err != nil {
			return task.Failed(err)
		}
		if err := w.writer.WriteLine(b); err != nil {
			return task.Failed(err)
		}
	}
	if err := rows.Close(); err != nil {
		return task.Failed(err)
	}

	// write to file
	if err := w.writer.Close(); err != nil {
		return task.Failed(err)
	}

	sts := w.writer.Stats()
	w.SetMeta("file", sts.Path)

	return task.Completed("%d rows written to %s (%s)", sts.LineCnt, sts.Path, humanize.Bytes(uint64(sts.ByteCnt)))
}

func (m FieldMap) convertRow(data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for key, value := range data {
		name := m[key]
		switch v := value.(type) {
		case []byte:
			s := string(v)
			result[name] = s
		default:
			result[name] = value
		}
	}

	return result
}
