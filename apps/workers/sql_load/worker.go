package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/jbsmith7741/uri"
	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
	"github.com/pkg/errors"
)

type InfoURI struct {
	FilePath  string            `uri:"origin"`                // file path to load one file or a list of files in that path (not recursive)
	Table     string            `uri:"table" required:"true"` // insert table name i.e., "schema.table_name"
	SkipErr   bool              `uri:"skip_err"`              // if bad records are found they are skipped and logged instead of throwing an error
	DeleteMap map[string]string `uri:"delete"`                // map used to build the delete query statement
	FieldsMap map[string]string `uri:"fields"`                // map json key values to different db names
}

type worker struct {
	options

	Params InfoURI

	flist   []string // list of full path file(s)
	records int64    // inserted records
	ds      *DataSet // the processing data for loading
	delete  string   // query statement built from DeleteMap
}

type Jsondata map[string]interface{}
type Row []interface{} // each row to be inserted
type Rows []Row        // all rows to be inserted
type Columns []string

type DataSet struct {
	jRow        Jsondata          // the current json data row to be added
	dbSchema    DbSchema          // the database schema for each column
	verified    bool              // has the jRow data been verified with the db columns
	insertCols  []string          // the actual db column names, must match dbrows
	insertKeys  []string          // the json keys mapping for the insert columns
	colTypes    []string          // the type of each column
	colNull     []bool            // is the column nullable
	insertRows  Rows              // all rows to be inserted
	fieldsMap   map[string]string // a copy of the fieldsMap from the worker uri params
	ignoredCols map[string]bool   // a list of db fields that were not inserted into
}

type DbColumn struct {
	Name       string
	FieldMap   string
	DataType   string
	IsNullable string
	Default    string
	TypeName   string // int64, float64, string
}

type DbSchema []DbColumn

var json = jsoniter.ConfigFastest

func (o *options) newWorker(info string) task.Worker {
	var err error

	w := &worker{
		options: *o,
		flist:   make([]string, 0),
		ds:      NewDataSet(),
	}

	if err := uri.Unmarshal(info, &w.Params); err != nil {
		return task.InvalidWorker("params uri.unmarshal: %v", err)
	}
	w.ds.fieldsMap = w.Params.FieldsMap
	// update any {colon} template with a real ":"
	for k, m := range w.Params.DeleteMap {
		w.Params.DeleteMap[k] = strings.Replace(m, "{colon}", ":", -1)
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

	w.deleteQuery()
	return w
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	// read the table schema to know the types for each column
	err := w.QuerySchema()
	if err != nil {
		return task.Failed(err)
	}

	// read the files for loading, verify columns types
	err = w.ReadFiles()
	if err != nil {
		return task.Failed(errors.Wrap(err, "readfiles error"))
	}

	b := db.NewBatchLoader(w.dbDriver, w.sqlDB)

	for _, row := range w.ds.insertRows {
		b.AddRow(row)
	}

	b.Delete(w.delete)

	stats, err := b.Commit(ctx, w.Params.Table, w.ds.insertCols...)
	if err != nil {
		return task.Failed(errors.Wrap(err, "commit to db failed"))
	}

	if stats.Removed > 0 {
		return task.Completed("database load completed %s table: %s records removed: %d records added: %d",
			w.dbDriver, w.Params.Table, stats.Removed, w.records)
	}

	return task.Completed("database load completed %s table: %s records: %d",
		w.dbDriver, w.Params.Table, w.records)
}

// Queries the database for the table schema for each column
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
			return errors.New("query_schema: cannot parse table name")
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
		return errors.Wrap(err, "query_schema: cannot get table columns")
	}

	c := DbColumn{}
	for rows.Next() {
		rows.Scan(
			&c.Name,
			&c.IsNullable,
			&c.DataType,
			&c.Default,
		)
		w.ds.dbSchema = append(w.ds.dbSchema, c)
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	if len(w.ds.dbSchema) == 0 {
		return errors.New("db schema was not loaded")
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

// ReadFiles uses the workers file list (flist) and file options to read each file
// it then builds the cols and rows for all file(s)
func (w *worker) ReadFiles() (err error) {
	// read each file
	for i := range w.flist {
		r, e := file.NewReader(w.flist[i], w.fileOpts) // create a new file reader
		if e != nil {
			err = errors.Wrap(e, "new reader error")
			continue
		}

		// read the lines of the file
		for {
			line, e := r.ReadLine()
			if e != nil {
				if e == io.EOF {
					break
				}
				err = errors.Wrap(e, "readline error: "+r.Stats().Path)
				continue
			}

			// get the json object from the line that was read from the file
			e = json.Unmarshal(line, &w.ds.jRow)
			if e != nil {
				err = errors.Wrap(e, "json.unmarshal error")
				continue
			}
			w.ds.verified = false // the new json data has not been verified yet

			e = w.ds.AddRow()
			if e != nil {
				err = errors.Wrap(e, "add row error")
				continue
			}
			w.records++
		}
		r.Close() // close the reader
	}

	if w.Params.SkipErr {
		if err != nil {
			log.Println("skipping error records", err)
			err = nil
		}
	}

	return err
}

func NewDataSet() *DataSet {
	return &DataSet{
		jRow:        make(Jsondata),
		dbSchema:    make(DbSchema, 0),
		insertRows:  make(Rows, 0),
		insertCols:  make([]string, 0),
		insertKeys:  make([]string, 0),
		colNull:     make([]bool, 0),
		colTypes:    make([]string, 0),
		ignoredCols: make(map[string]bool),
	}
}

// VerifyRow will check the dataset insertCols and the jRow data to make sure
// all fields are accounted for, if it cannot find a db col in the jRow
// it will set that missing jRow value to nil if it's nullable in the db
// it will also check the json jRow key values against the cols list
func (ds *DataSet) VerifyRow() (err error) {
	if len(ds.jRow) == 0 {
		return errors.New("no data found in json jRow object")
	}

	// for the json record, add the json data keys
	// but only where the column was found in the database schema
	for k := range ds.jRow {
		c, foundDb := ds.findDbColumn(k)
		foundCol := ds.findInsertKey(k)
		if foundDb && !foundCol {
			ds.insertKeys = append(ds.insertKeys, k)          // json key names
			ds.insertCols = append(ds.insertCols, c.FieldMap) // db column names
			ds.colTypes = append(ds.colTypes, c.TypeName)
			ds.colNull = append(ds.colNull, c.IsNullable == "YES")
		}
	}

	ds.defaultUpdate()

	ds.verified = true
	return nil
}

// if the database table field is nullable, and it's not in the JSON string
// and it's part of the insertCols, the value will be nil (NULL)
// if the field is NOT nullable, the value will be the zero value
func (ds *DataSet) defaultUpdate() (err error) {
	// has the column fields count changed?
	for idr := range ds.insertRows { // loop though the rows
		rowRecords := len(ds.insertRows[idr])
		colsCount := len(ds.insertCols)
		// there are more columns to insert than data values in the row
		if colsCount > rowRecords {
			// start at the new row value, add columns untill row values count matches the column count
			for idx := rowRecords; idx < colsCount; idx++ {
				if ds.colNull[idx] {
					ds.insertRows[idr] = append(ds.insertRows[idr], nil)
				} else {
					if ds.colTypes[idx] == "string" {
						ds.insertRows[idr] = append(ds.insertRows[idr], "")
					}
					if ds.colTypes[idx] == "int" || ds.colTypes[idx] == "float" {
						ds.insertRows[idr] = append(ds.insertRows[idr], 0)
					}
				}
			}
		}
	}

	return nil
}

// Takes the insert columns and the json byte data (jRow) and adds to the Dataset rows slice
// an error is returned if the row cannot be added to the DataSet rows
func (ds *DataSet) AddRow() (err error) {
	if ds.verified == false {
		err = ds.VerifyRow()
		if err != nil {
			return err
		}
	}

	row := make(Row, len(ds.insertKeys))
	for k, f := range ds.insertKeys {
		v, _ := ds.jRow[f] // search the json object for the insert key

		switch x := v.(type) {
		case string:
			if ds.colTypes[k] == "int" {
				ds.jRow[f], err = strconv.ParseInt(x, 10, 64)
			}
			if ds.colTypes[k] == "float" {
				ds.jRow[f], err = strconv.ParseFloat(x, 64)
			}
		case float64:
			// convert a float to an int if the schema is an int type
			if ds.colTypes[k] == "int" {
				if x == float64(int64(x)) {
					ds.jRow[f] = int64(x)
				} else {
					return errors.New(
						fmt.Sprintf("add_row: cannot convert number value to int64 for %s value: %v type: %s", f, ds.jRow[f], ds.colTypes[k]))
				}
			}
		}
		if err != nil {
			return err
		}
		row[k] = ds.jRow[f]
	}

	ds.insertRows = append(ds.insertRows, row)

	return nil
}

func (w *worker) deleteQuery() string {
	if len(w.Params.DeleteMap) == 0 {
		return ""
	}
	s := make([]string, 0)
	for k, v := range w.Params.DeleteMap {
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
	w.delete = fmt.Sprintf("delete from %s where %s", w.Params.Table, strings.Join(s, " and "))
	return w.delete
}

// findDbColumn will return the schema, and true if the name was found in the
// dbColumn slice, false if not found
// the TypeName will be updated once if it hasn't been updated based on the schema column type
func (ds *DataSet) findDbColumn(name string) (dbc DbColumn, found bool) {
	// check the fields map if they exist in the DB schema

	if ds.ignoredCols[name] {
		return dbc, false
	}

	n, ok := ds.fieldsMap[name]
	if ok {
		name = n
	}

	// check for the
	for _, c := range ds.dbSchema {
		if c.Name == name {
			c.FieldMap = name
			return c, true
		}
	}

	ds.ignoredCols[name] = true

	return dbc, false
}

// findInsertCol will return true if the name was found in the list of insert columns
func (ds *DataSet) findInsertKey(name string) bool {
	for _, v := range ds.insertKeys {
		if v == name {
			return true
		}
	}
	return false
}
