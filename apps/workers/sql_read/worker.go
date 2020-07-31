package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jbsmith7741/uri"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/file"
)

type worker struct {
	task.Meta
	db *sqlx.DB

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
		Table     string            `uri:"table" required:"true"`
		QueryFile string            `uri:"origin"` // path to query file
		Fields    map[string]string `uri:"field"`
	}{}
	if err := uri.Unmarshal(info, &iOpts); err != nil {
		return task.InvalidWorker(err.Error())
	}
	if iOpts.QueryFile == "" && len(iOpts.Fields) == 0 {
		return task.InvalidWorker("query file or fields values is required")
	}

	// todo move db connection outside to handle closing properly
	// setup database connection
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", o.Username, o.Password, o.Host, o.DBName)
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	// pull info about table
	s := strings.Split(iOpts.Table, ".")
	if len(s) != 2 {
		return task.InvalidWorker("table requires schema and table (schema.table)")
	}
	rows, err := db.Query("SELECT column_name, data_type\n FROM information_schema.columns WHERE table_schema = ? AND table_name = ?", s[0], s[1])

	if err != nil {
		return task.InvalidWorker(err.Error())
	}
	fields := make(map[string]*Field)
	for rows.Next() {
		f := &Field{}
		if err := rows.Scan(&f.Name, &f.DataType); err != nil {
			return task.InvalidWorker(err.Error())
		}
		fields[f.Name] = f
	}
	rows.Close()

	for k, v := range iOpts.Fields {
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

	return &worker{
		Meta:   task.NewMeta(),
		db:     db,
		Fields: fields,
		Query:  query,
	}
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	// pull Data from mysql database
	rows, err := w.db.QueryxContext(ctx, w.Query)
	if err != nil {
		return task.Failed(err)
	}
	out, _ := file.NewWriter("./tmp.json", nil)
	log.Println(w.Query)
	log.Println(rows.Columns())
	for rows.Next() {
		row := make(map[string]interface{})
		rows.MapScan(row)

		r := w.Fields.convertRow(row)
		b, err := jsoniter.Marshal(r)
		if err != nil {
			return task.Failed(err)
		}
		if err := out.WriteLine(b); err != nil {
			return task.Failed(err)
		}
	}
	if err := rows.Close(); err != nil {
		return task.Failed(err)
	}
	if err := out.Close(); err != nil {
		return task.Failed(err)
	}

	// process results as needed

	// load data into bigquery table or GCS
	return task.Completed("data written")
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
