package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
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

type FieldMap map[string]*Field

type Field struct {
	DataType string
	Name     string
}

func (o *options) NewWorker(info string) task.Worker {
	// unmarshal info string
	iOpts := struct {
		Table       string            `uri:"table" required:"true"`
		QueryFile   string            `uri:"origin"` // path to query file
		Fields      map[string]string `uri:"field"`
		Destination string            `uri:"dest" required:"true"`
	}{}
	if err := uri.Unmarshal(info, &iOpts); err != nil {
		return task.InvalidWorker(err.Error())
	}

	// pull info about table
	s := strings.Split(iOpts.Table, ".")
	if len(s) != 2 {
		return task.InvalidWorker("table requires schema and table (schema.table)")
	}
	rows, err := o.db.Query("SELECT column_name, data_type\n FROM information_schema.columns WHERE table_schema = ? AND table_name = ?", s[0], s[1])

	if err != nil {
		return task.InvalidWorker(err.Error())
	}
	fields := make(map[string]*Field)
	defer rows.Close()
	for rows.Next() {
		f := &Field{}
		if err := rows.Scan(&f.Name, &f.DataType); err != nil {
			return task.InvalidWorker(err.Error())
		}
		fields[f.Name] = f
	}
	for k, v := range iOpts.Fields {
		if _, found := fields[k]; !found {
			return task.InvalidWorker("invalid column: '%s'", k)
		}
		fields[k].Name = v
	}

	var query string
	// get query
	if len(iOpts.Fields) > 0 {
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
		Fields: fields,
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
		name := m[key].Name
		switch v := value.(type) {
		case []byte:
			s := string(v)
			switch m[key].DataType {
			case "int", "tinyint", "mediumint":
				i, err := strconv.Atoi(s)
				if err != nil {
					log.Println(err)
				}
				result[name] = i
			default:
				result[name] = s
			}
		default:
			result[name] = value
		}
	}

	return result
}
