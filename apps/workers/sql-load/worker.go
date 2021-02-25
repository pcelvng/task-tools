package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gtools "github.com/jbsmith7741/go-tools"
	"github.com/jbsmith7741/uri"
	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
	"github.com/zenthangplus/goccm"
)

type InfoURI struct {
	FilePath     string            `uri:"origin"`                // file path to load one file or a list of files in that path (not recursive)
	Table        string            `uri:"table" required:"true"` // insert table name i.e., "schema.table_name"
	SkipErr      bool              `uri:"skip_err"`              // if bad records are found they are skipped and logged instead of throwing an error
	DeleteMap    map[string]string `uri:"delete"`                // map used to build the delete query statement
	FieldsMap    map[string]string `uri:"fields"`                // map json key values to different db names
	Truncate     bool              `uri:"truncate"`              // truncate the table rather than delete
	CachedInsert bool              `uri:"cached_insert"`         // this will attempt to load the query data though a temp table (postgres only)
}

type worker struct {
	options
	task.Meta

	Params InfoURI

	flist    []string // list of full path file(s)
	ds       *DataSet // the processing data for loading
	delQuery string   // query statement built from DeleteMap

	queryBuildTime time.Duration // query building time
	queryRunTime   time.Duration // query running time
}

type Jsondata map[string]interface{}
type Row []interface{} // each row to be inserted

type DataSet struct {
	dbSchema   []DbColumn // the database schema for each column
	insertCols []string   // the actual db column names, must match dbrows
	rowCount   int32

	//mux sync.Mutex // needed to thread add row
}

type DbColumn struct {
	Name       string  // DB column name
	DataType   string  // DB data type
	IsNullable string  // DB YES or NO string values
	Default    *string // DB default function / value
	TypeName   string  // string, int, float
	JsonKey    string  // matching json key name
	Nullable   bool    // bool value if column is nullable (true) or not (false)
}

var json = jsoniter.ConfigFastest

func (o *options) newWorker(info string) task.Worker {
	w := &worker{
		options: *o,
		Meta:    task.NewMeta(),
		flist:   make([]string, 0),
		ds:      NewDataSet(),
	}

	if err := uri.Unmarshal(info, &w.Params); err != nil {
		return task.InvalidWorker("params uri.unmarshal: %v", err)
	}

	f, err := file.Stat(w.Params.FilePath, w.fileOpts)
	if err != nil {
		return task.InvalidWorker("filepath os: %v", err)
	}
	// app will load one file or a directory of files (only one folder deep)
	if f.IsDir {
		list, _ := file.List(w.Params.FilePath, w.fileOpts)
		for i := range list {
			w.flist = append(w.flist, list[i].Path)
		}
	} else {
		w.flist = append(w.flist, w.Params.FilePath)
	}

	if len(w.flist) == 0 {
		return task.InvalidWorker("no files found in path %s", w.Params.FilePath)
	}
	if len(w.Params.DeleteMap) > 0 && w.Params.Truncate {
		return task.InvalidWorker("truncate can not be used with delete fields")
	}
	w.delQuery = DeleteQuery(w.Params.DeleteMap, w.Params.Table)
	if w.Params.Truncate {
		w.delQuery = fmt.Sprintf("delete from %s", w.Params.Table)
	}
	return w
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	// read the table schema to know the types for each column
	err := w.QuerySchema()
	if err != nil {
		return task.Failed(err)
	}

	// read the files for loading, verify columns types

	errChan := make(chan error)
	rowChan := make(chan Row, 100)
	w.ds.dbSchema, w.ds.insertCols = PrepareMeta(w.ds.dbSchema, w.Params.FieldsMap)
	log.Println(len(w.ds.insertCols), w.ds.insertCols)

	go w.ds.ReadFiles(ctx, w.flist, w.fileOpts, errChan, rowChan)
	go func() {
		for e := range errChan {
			log.Println(e)
		}
	}()
	retry := 0

	if w.Params.CachedInsert && w.dbDriver == "postgres" {
		start := time.Now()
		q, err := w.ds.QueryString(rowChan, w.Params.Table, w.delQuery, w.Params.Truncate)
		if err != nil {
			return task.Failed(err)
		}
		if task.IsDone(ctx) {
			return task.Interrupted()
		}
		w.queryBuildTime = time.Now().Sub(start)

		var txErr error
		var tx *sql.Tx

		start = time.Now()
		for {
			if retry > 2 { // we will retry the transaction 3 times only
				break
			}
			tx, txErr = w.sqlxDB.BeginTx(ctx, &sql.TxOptions{})
			if txErr != nil {
				return task.Failed(fmt.Errorf("failed to start transaction %w", err))
			}
			_, txErr = tx.ExecContext(ctx, q)
			if txErr != nil {
				tx.Rollback()
				retry++
			} else {
				tx.Commit()
				break
			}
		}

		if txErr != nil {
			return task.Failed(fmt.Errorf("transaction failed %w", txErr))
		}

		w.queryRunTime = time.Now().Sub(start)
	} else {
		start := time.Now()
		b := db.NewBatchLoader(w.dbDriver, w.sqlDB)

		for row := range rowChan {
			atomic.AddInt32(&w.ds.rowCount, 1)
			b.AddRow(row)
		}
		b.Delete(w.delQuery)
		w.queryBuildTime = time.Now().Sub(start)
		start = time.Now()
		stats, err := b.Commit(ctx, w.Params.Table, w.ds.insertCols...)
		if err != nil {
			return task.Failed(fmt.Errorf("commit to db failed %w", err))
		}
		w.queryRunTime = time.Now().Sub(start)
		if stats.Removed > 0 {
			w.SetMeta("removed_records", fmt.Sprintf("%d", stats.Removed))
		}
	}

	w.SetMeta("query_build_time", fmt.Sprintf("%v", gtools.PrintDuration(w.queryBuildTime)))
	w.SetMeta("insert_records", fmt.Sprintf("%d", w.ds.rowCount))
	w.SetMeta("query_run_time", fmt.Sprintf("%v", gtools.PrintDuration(w.queryRunTime)))

	return task.Completed("database load completed %s table: %s records: %d tried: %d",
		w.dbDriver, w.Params.Table, w.ds.rowCount, retry+1)
}

// QuerySchema queries the database for the table schema for each column
// sets the worker's db value
func (w *worker) QuerySchema() (err error) {
	var t, s string // table and schema

	q := `SELECT column_name, is_nullable, data_type, column_default
 FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'`

	// split the table name into the schema and table name seperately
	if w.dbDriver == "postgres" {
		n := strings.Split(w.Params.Table, ".")
		if len(n) == 1 {
			s = "public"
			t = w.Params.Table
		} else if len(n) == 2 {
			s = n[0]
			t = n[1]
		} else {
			return fmt.Errorf("query_schema: cannot parse table name")
		}
	}

	// the "schema" is actually the database name in mysql
	if w.dbDriver == "mysql" {
		s = w.MySQL.DBName
		t = w.Params.Table
	}

	query := fmt.Sprintf(q, s, t)
	rows, err := w.sqlDB.Query(query)
	if err != nil {
		return fmt.Errorf("query_schema: cannot get table columns %w", err)
	}

	c := DbColumn{}
	var isNullable string
	for rows.Next() {
		err := rows.Scan(
			&c.Name,
			&isNullable,
			&c.DataType,
			&c.Default,
		)
		c.Nullable = isNullable == "YES"
		if err != nil {
			log.Println(err)
		}
		w.ds.dbSchema = append(w.ds.dbSchema, c)
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	if len(w.ds.dbSchema) == 0 {
		return fmt.Errorf("db schema was not loaded")
	}

	for idx, c := range w.ds.dbSchema {
		if c.TypeName == "" {
			if strings.Contains(c.DataType, "char") || strings.Contains(c.DataType, "text") {
				w.ds.dbSchema[idx].TypeName = "string"
			}

			if strings.Contains(c.DataType, "int") || strings.Contains(c.DataType, "serial") {
				w.ds.dbSchema[idx].TypeName = "int"
			}

			if strings.Contains(c.DataType, "numeric") || strings.Contains(c.DataType, "dec") ||
				strings.Contains(c.DataType, "double") || strings.Contains(c.DataType, "real") ||
				strings.Contains(c.DataType, "fixed") || strings.Contains(c.DataType, "float") {
				w.ds.dbSchema[idx].TypeName = "float"
			}
		}
	}
	return nil
}

// ReadFiles uses a files list and file.Options to read files and process data into a Dataset
// it will build the cols and rows for each file
func (ds *DataSet) ReadFiles(ctx context.Context, files []string, fOpts *file.Options, errChan chan error, rowChan chan Row) {
	// read each file
	for i := range files {
		r, err := file.NewReader(files[i], fOpts) // create a new file reader
		if err != nil {
			errChan <- fmt.Errorf("new reader error %w", err)
			continue
		}
		c := goccm.New(20)
		// read the lines of the file
		for {
			if task.IsDone(ctx) {
				defer func() {
					c.WaitAllDone()
					close(rowChan)
				}()
				return
			}
			line, e := r.ReadLine()
			if e != nil {
				if e == io.EOF {
					break
				}
				errChan <- fmt.Errorf("readline error %v - %w", r.Stats().Path, err)
				continue
			}
			c.Wait()
			go func(b []byte) {
				var j Jsondata
				e := json.Unmarshal(b, &j)
				if e != nil {
					err = fmt.Errorf("json unmarshal error %w", e)
				} else {
					if row, err := ds.AddRow(j); err != nil {
						errChan <- err
					} else if row != nil {
						rowChan <- row
					}
				}
				c.Done()
			}(line)
		}

		c.WaitAllDone()
		close(rowChan)
		close(errChan)
		log.Println("processed file", r.Stats().Path)
		r.Close() // close the reader
	}

	/*if skipErr {
		if err != nil {
			log.Println("skipping error records", err)
			err = nil
		}
	}

	return err*/
}

func NewDataSet() *DataSet {
	return &DataSet{
		dbSchema:   make([]DbColumn, 0),
		insertCols: make([]string, 0),
	}
}

// PrepareMeta will check the dataset insertCols and the json data to make sure
// all fields are accounted for, if it cannot find a db col in the jRow
// it will set that missing jRow value to nil if it's nullable in the db
// it will also check the json jRow key values against the cols list
func PrepareMeta(dbSchema []DbColumn, fieldMap map[string]string) (meta []DbColumn, cols []string) {
	// for the json record, add the json data keys
	// but only where the column was found in the database schema
	for _, k := range dbSchema {
		jKey := k.Name
		if v := fieldMap[k.Name]; v != "" {
			jKey = v
		}
		// skip designated fields
		if jKey == "-" {
			continue
		}
		// skip columns that have functions associated with them
		if k.Default != nil && strings.Contains(*k.Default, "(") &&
			strings.Contains(*k.Default, ")") {
			continue
		}
		cols = append(cols, k.Name) // db column names
		k.JsonKey = jKey
		meta = append(meta, k)
	}

	return meta, cols
}

// AddRow Takes the insert columns and the json byte data (jRow) and adds to the Dataset rows slice
// an error is returned if the row cannot be added to the DataSet rows
func (ds *DataSet) AddRow(j Jsondata) (row Row, err error) {

	row = make(Row, len(ds.insertCols))

	for k, f := range ds.dbSchema {
		v := j[f.JsonKey]
		switch x := v.(type) {
		case string:
			if ds.dbSchema[k].TypeName == "int" {
				j[f.JsonKey], err = strconv.ParseInt(x, 10, 64)
			}
			if ds.dbSchema[k].TypeName == "float" {
				j[f.JsonKey], err = strconv.ParseFloat(x, 64)
			}
		case float64:
			// convert a float to an int if the schema is an int type
			if ds.dbSchema[k].TypeName == "int" {
				if x != float64(int64(x)) {
					err = fmt.Errorf("add_row: cannot convert number value to int64 for %s value: %v type: %s",
						f.Name, v, ds.dbSchema[k].DataType)
				}
				j[f.JsonKey] = int64(x)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("add row  %w", err)
		}
		row[k] = j[f.JsonKey]
	}
	return row, nil
}

func DeleteQuery(m map[string]string, table string) string {
	if len(m) == 0 {
		return ""
	}
	s := make([]string, 0)
	for k, v := range m {
		isString := true
		_, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			isString = false
		}
		_, err = strconv.ParseFloat(v, 64)
		if err == nil {
			isString = false
		}

		if isString {
			s = append(s, k+" = '"+v+"'")
		} else {
			s = append(s, k+" = "+v)
		}
	}

	sort.Sort(sort.StringSlice(s))
	return fmt.Sprintf("delete from %s where %s", table, strings.Join(s, " and "))
}

func RandString(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// QueryString will take DataSet data and build a query string with a temp table for inserting,
// this is to test improving the loading times for the the insert statements,
// at the moment this is only tested and works with postgres
func (ds *DataSet) QueryString(rowChan chan Row, tableName, deleteQuery string, truncate bool) (q string, err error) {
	c := goccm.New(20)

	type Query struct {
		bytes.Buffer

		mux sync.Mutex
	}

	qry := Query{}

	for rs := range rowChan {
		atomic.AddInt32(&ds.rowCount, 1)
		c.Wait()
		go func(row Row) {
			var f bytes.Buffer
			f.WriteString("(")
			for ir, r := range row {
				switch x := r.(type) {
				case int64:
					f.WriteString(strconv.FormatInt(x, 10))
				case int:
					f.WriteString(strconv.Itoa(x))
				case float64:
					f.WriteString(strconv.FormatFloat(x, 'f', -1, 64))
				case string:
					f.WriteString("'")
					f.WriteString(strings.Replace(x, "'", "''", -1))
					f.WriteString("'")
				case bool:
					if x {
						f.WriteString("true")
					} else {
						f.WriteString("false")
					}
				default:
					if x == nil {
						f.WriteString("NULL")
					} else {
						log.Printf("exec_query: non-nil default type error value[%v] type[%T]\n", x, x)
					}
				}

				if ir < len(row)-1 {
					f.WriteString(",")
				}
			}

			f.WriteString("),\n")

			qry.mux.Lock()
			qry.WriteString(f.String())
			qry.mux.Unlock()
			c.Done()
		}(rs)
	}

	c.WaitAllDone()

	qry.Truncate(qry.Len() - 2)
	qry.WriteString(";\n")

	// replace any dots in the name so this can be a session temp table
	t := strings.Replace(tableName, ".", "_", -1) + "_" + RandString(10)
	fields := strings.Join(ds.insertCols, ",")

	header := "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n" +
		"create temp table " + t + " as table " + tableName + " with no data;\n" +
		"insert into " + t + "(" + fields + ")\n" +
		"  VALUES \n"

	if deleteQuery != "" {
		qry.WriteString(deleteQuery + ";\n")
	}
	if truncate {
		qry.WriteString("delete from " + tableName + ";\n")
	}

	qry.WriteString("insert into " + tableName + "(" + fields + ")\n select " + fields + " from " + t + ";\n")
	qry.WriteString("COMMIT;")
	return header + qry.String(), nil
}
