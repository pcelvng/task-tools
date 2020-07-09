package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/jbsmith7741/uri"
	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pkg/errors"
)

type InfoOptions struct {
	FilePath string `uri:"origin"`                // file path to load one file or a list of files in that path (not recursive)
	Table    string `uri:"table" required:"true"` // insert table name i.e., "schema.table_name"
	SkipErr  bool   `uri:"skip_err"`              // if bad records are found they are skipped and logged instead of throwing an error
}

type worker struct {
	options

	Params InfoOptions

	flist   []stat.Stats // list of file(s)
	records int64        // inserted records
	ds      *DataSet     // the processing data for loading
}

type Jsondata map[string]interface{}
type Row []interface{}
type Rows []Row
type Columns []string

type DataSet struct {
	jRow       Jsondata // the current json data row to be added
	dbSchema   DbSchema // the database schema for each column
	verified   bool     // has the jRow data been verified with the db columns
	insertCols []string // column field names all must be accounted for in the Rows
	insertRows Rows     // all rows to be inserted
}

type DbColumn struct {
	Name       string
	DataType   string
	IsNullable string
}

type DbSchema []DbColumn

var json = jsoniter.ConfigFastest

func (o *options) newWorker(info string) task.Worker {
	var err error

	w := &worker{
		options: *o,
		flist:   make([]stat.Stats, 0),
		ds:      NewDataSet(),
	}

	if err := uri.Unmarshal(info, &w.Params); err != nil {
		return task.InvalidWorker("params uri.unmarshal: %v", err)
	}

	f, err := os.Stat(w.Params.FilePath)
	if err != nil {
		return task.InvalidWorker("filepath os: %v", err)
	}
	// app will load one file or a directory of files (only one folder deep)
	switch mode := f.Mode(); {
	case mode.IsDir():
		w.flist, _ = file.List(w.Params.FilePath, w.fileOpts)
	case mode.IsRegular():
		s, _ := file.Stat(w.Params.FilePath, w.fileOpts)
		w.flist = append(w.flist, s.Clone())
	}
	if len(w.flist) == 0 {
		return task.InvalidWorker("no files found in path %s", w.Params.FilePath)
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
	err = w.ReadFiles()
	if err != nil {
		return task.Failed(errors.Wrap(err, "readfiles error"))
	}

	b := db.NewBatchLoader(w.dbDriver, w.sqlDB)

	for _, row := range w.ds.insertRows {
		b.AddRow(row)
	}

	b.Commit(ctx, w.Params.Table, w.ds.insertCols...)

	return task.Completed("completed")
}

// Queries the database for the table schema for each column
// sets the worker's db value
func (w *worker) QuerySchema() (err error) {
	var t, s string // table and schema

	q := `SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'`

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
		)
		w.ds.dbSchema = append(w.ds.dbSchema, c)
	}
	return nil
}

// ReadFiles uses the workers file list (flist) and file options to read each file
// it then builds the cols and rows for all file(s)
func (w *worker) ReadFiles() (err error) {
	// read each file
	for i := range w.flist {
		f := w.flist[i].Clone()
		r, e := file.NewReader(f.Path, w.fileOpts) // create a new file reader
		if e != nil {
			err = errors.Wrap(e, "new reader error\n")
			continue
		}

		// read the lines of the file
		for {
			line, e := r.ReadLine()
			if e != nil {
				if e == io.EOF {
					break
				}
				err = errors.Wrap(e, "readline error: "+r.Stats().Path+"\n")
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
				err = errors.Wrap(e, fmt.Sprintf("%+v", w.ds.jRow)+"\n")
				continue
			}
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
		jRow:       make(Jsondata),
		dbSchema:   make(DbSchema, 0),
		insertRows: make(Rows, 0),
		insertCols: make([]string, 0),
	}
}

// Verify will check the dataset insertCols and the jRow data to make sure
// all fields are accounted for, if it cannot find a db col in the jRow
// it will set that missing jRow value to nil if it's nullable in the db
// it will also check the json jRow key values against the cols list
func (ds *DataSet) VerifyRow() (err error) {
	if len(ds.jRow) == 0 {
		return errors.New("no data found in json jRow object")
	}

	// if the insert columns have not been set
	// set them based on the json row data and the db schema
	if len(ds.insertCols) == 0 {
		for k := range ds.jRow {
			// only add the json data keys where the column was found in the database schema
			_, err := ds.dbSchema.GetColumn(k)
			if err == nil {
				ds.insertCols = append(ds.insertCols, k)
			}
		}
		// verify all nullable fields have been added to the cols list
		for _, d := range ds.dbSchema {
			found := false
			for _, c := range ds.insertCols {
				if c == d.Name {
					found = true
					break // the field was found in the db schema
				}
			}
			// if the column was not found in the
			// add the db column to the cols list as long as it's nullable
			if !found {
				if d.IsNullable == "YES" {
					ds.insertCols = append(ds.insertCols, d.Name)
				} else {
					// if the column was not found in the insert columns list, and the column is not nullable
					return errors.New("missing key for non-nullable field: " + d.Name)
				}
			}
		}
	}

	err = ds.SetNullValues()
	if err != nil {
		return err
	}

	ds.verified = true
	return nil
}

// loop though all the insert columns,
// if that column isn't found in the json row data
// set that key's value in the json row to nil
func (ds *DataSet) SetNullValues() (err error) {
	// are all the insert columns found in the json dataset
	for _, c := range ds.insertCols {
		_, ok := ds.jRow[c] // look for the column key in the json object
		if !ok {
			col, err := ds.dbSchema.GetColumn(c)
			if err != nil {
				return err
			}
			if col.IsNullable != "YES" {
				return errors.New("missing data for non-nullable field: " + c)
			}
			ds.jRow[c] = nil
		}
	}
	return nil
}

// returns the column schema information for a specific column name
func (dbcs DbSchema) GetColumn(name string) (dbc DbColumn, err error) {
	for _, v := range dbcs {
		if v.Name == name {
			return v, nil
		}
	}
	return dbc, errors.New("get_column not found: " + name)
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

	row := make(Row, 0)
	for _, f := range ds.insertCols {
		c, _ := ds.dbSchema.GetColumn(f)

		v, _ := ds.jRow[f]
		switch x := v.(type) {
		case string:
			if !strings.Contains(c.DataType, "char") && c.DataType != "text" {
				return errors.New(
					fmt.Sprintf("add_row: cannot convert string to a number for: %s value: %v type: %s", f, ds.jRow[f], c.DataType))
			}
		case float64:
			if strings.Contains(c.DataType, "int") {
				if x == float64(int64(x)) {
					ds.jRow[f] = int64(x)
				} else {
					return errors.New(
						fmt.Sprintf("add_row: cannot convert number value to int64 for %s value: %v type: %s", f, ds.jRow[f], c.DataType))
				}
			}
		}
		row = append(row, ds.jRow[f])
	}

	ds.insertRows = append(ds.insertRows, row)

	return nil
}
