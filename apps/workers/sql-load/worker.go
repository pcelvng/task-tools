package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gtools "github.com/jbsmith7741/go-tools"
	"github.com/jbsmith7741/uri"
	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
)

type InfoURI struct {
	FilePath     string            `uri:"origin"`                     // file path to load one file or a list of files in that path (not recursive)
	Table        string            `uri:"table" required:"true"`      // insert table name i.e., "schema.table_name"
	SkipErr      bool              `uri:"skip_err"`                   // if bad records are found they are skipped and logged instead of throwing an error
	DeleteMap    map[string]string `uri:"delete"`                     // map used to build the delete query statement
	FieldsMap    map[string]string `uri:"fields"`                     // map json key values to different db names
	Truncate     bool              `uri:"truncate"`                   // truncate the table rather than delete
	CachedInsert bool              `uri:"cached_insert"`              // this will attempt to load the query data though a temp table (postgres only)
	BatchSize    int               `uri:"batch_size" default:"10000"` // number of rows to insert at once
	CSV          bool              `uri:"csv" default:"false"`        // parse csv data instead of json data
	Delimiter    string            `uri:"delimiter" default:","`      // csv delimiter, default is a comma
}

type worker struct {
	options
	task.Meta

	Params InfoURI

	//flist    []string // list of full path file(s)
	fReader  file.Reader
	ds       *DataSet // the processing data for loading
	delQuery string   // query statement built from DeleteMap

	queryRunTime time.Duration // query running time
}

type JsonData map[string]interface{}
type CsvData []string
type Row []interface{} // each row to be inserted

type DataSet struct {
	dbSchema   []DbColumn // the database schema for each column
	insertCols []string   // the actual db column names, must match dbrows
	rowCount   int32
	skipCount  int

	csvData   bool // is the dataset for csv data (not json)
	delimiter rune // csv delimiter value default is comma

	err error

	//mux sync.RWMutex // needed to thread add row
}

type DbColumn struct {
	Name       string  // DB column name
	DataType   string  // DB data type
	IsNullable string  // DB YES or NO string values
	Default    *string // DB default function / value
	TypeName   string  // string, int, float
	FieldKey   string  // matching json or csv header key name
	Nullable   bool    // bool value if column is nullable (true) or not (false)
}

var json = jsoniter.ConfigFastest

func (o *options) newWorker(info string) task.Worker {
	w := &worker{
		options: *o,
		Meta:    task.NewMeta(),
	}

	if err := uri.Unmarshal(info, &w.Params); err != nil {
		return task.InvalidWorker("params uri.unmarshal: %v", err)
	}

	w.ds = NewDataSet(w.Params.CSV, []rune(w.Params.Delimiter)[0])

	r, err := file.NewGlobReader(w.Params.FilePath, w.fileOpts)
	if err != nil {
		return task.InvalidWorker("%v", err)
	}
	w.fReader = r

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
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	// read the files for loading, verify columns types

	rowChan := make(chan Row, 100)
	w.ds.dbSchema, w.ds.insertCols = PrepareMeta(w.ds.dbSchema, w.Params.FieldsMap)

	go w.ds.ReadFiles(ctx, w.fReader, rowChan, w.Params.SkipErr)
	retry := 0

	if w.Params.CachedInsert && w.dbDriver == "postgres" {
		start := time.Now()

		// create table
		tempTable := strings.Replace(w.Params.Table, ".", "_", -1) + "_" + RandString(10)
		createTempTable := "create temp table " + tempTable + " as table " + w.Params.Table + " with no data;\n"

		defer func() {
			if _, err := w.sqlDB.Exec("drop table if exists " + tempTable); err != nil {
				log.Println(err)
			}
		}()

		// create batched inserts
		queryChan := make(chan string, 10)
		go CreateInserts(rowChan, queryChan, tempTable, w.ds.insertCols, w.Params.BatchSize)

		tableCreated := false
		// load data into temp table
		for s := range queryChan {
			if !tableCreated {
				s = createTempTable + s
				tableCreated = true
			}
			if _, err := w.sqlDB.ExecContext(ctx, s); err != nil {
				cancelFn()
				return task.Failed(err)
			}
		}

		if w.ds.err != nil {
			return task.Failed(w.ds.err)
		}

		if !tableCreated {
			return task.Completed("no data to load for %s", w.Params.Table)
		}

		//finalize and transfer data
		var txErr error
		var tx *sql.Tx

		start = time.Now()
		q := "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n"

		if w.delQuery != "" {
			q += w.delQuery + ";\n"
		} else if w.Params.Truncate {
			q += "delete from " + w.Params.Table + ";\n"
		}
		fields := strings.Join(w.ds.insertCols, ",")
		q += "insert into " + w.Params.Table + "(" + fields + ")\n select " + fields + " from " + tempTable + ";\n" + "COMMIT;"
		for ; retry <= 2; retry++ {
			if retry > 2 { // we will retry the transaction 3 times only
				break
			}
			tx, txErr = w.sqlDB.BeginTx(ctx, &sql.TxOptions{})
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

		w.queryRunTime = time.Since(start)
	} else {
		start := time.Now()
		b := db.NewBatchLoader(w.dbDriver, w.sqlDB)

		for row := range rowChan {
			atomic.AddInt32(&w.ds.rowCount, 1)
			b.AddRow(row)
		}
		b.Delete(w.delQuery)
		start = time.Now()
		stats, err := b.Commit(ctx, w.Params.Table, w.ds.insertCols...)
		if err != nil {
			return task.Failed(fmt.Errorf("commit to db failed %w", err))
		}
		w.queryRunTime = time.Since(start)
		if stats.Removed > 0 {
			w.SetMeta("removed_records", fmt.Sprintf("%d", stats.Removed))
		}
	}

	w.SetMeta("insert_records", fmt.Sprintf("%d", w.ds.rowCount))
	w.SetMeta("query_run_time", fmt.Sprintf("%v", gtools.PrintDuration(w.queryRunTime)))
	w.SetMeta("transaction_attempt", strconv.Itoa(retry))
	if w.Params.SkipErr {
		w.SetMeta("skipped_rows", strconv.Itoa(w.ds.skipCount))
	}

	return task.Completed("database load completed %s table: %s records: %d",
		w.dbDriver, w.Params.Table, w.ds.rowCount)
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
func (ds *DataSet) ReadFiles(ctx context.Context, files file.Reader, rowChan chan Row, skipErrors bool) {
	errChan := make(chan error, 20)
	dataIn := make(chan []byte, 20)
	var header []string
	var hBytes []byte
	var activeThreads int32

	// read the first data bytes to capture the header for csv data

	for i := 0; i < 20; i++ {
		activeThreads++
		go func() { // csv data parsing
			for b := range dataIn { // should block until it gets data
				if ds.csvData {
					if bytes.Equal(b, hBytes) {
						continue // another header row was found skip
					}
					fmt.Println("threads header debug", header)
					if row, e := MakeCsvRow(ds.dbSchema, b, header, ds.delimiter); e != nil {
						fmt.Println("debug error")
						errChan <- fmt.Errorf("csv read error %w %q", e, string(b))
					} else if row != nil {
						fmt.Println("sending row")
						atomic.AddInt32(&ds.rowCount, 1)
						rowChan <- row
					}
				} else { // json data parsing
					var j JsonData
					if e := json.Unmarshal(b, &j); e != nil {
						errChan <- fmt.Errorf("json unmarshal error %w %q", e, string(b))
						return
					}

					if row, err := MakeRow(ds.dbSchema, j); err != nil {
						errChan <- fmt.Errorf("%w", err)
					} else if row != nil {
						atomic.AddInt32(&ds.rowCount, 1)
						rowChan <- row
					}
				}
			}
		}()
	}

	// read the lines of the file
	h := true
	scanner := file.NewScanner(files)
loop:
	for scanner.Scan() {

		if ds.csvData && h {
			var err error
			hBytes = scanner.Bytes()
			header, err = MakeCsvHeader(hBytes, ds.delimiter)
			if err != nil {
				ds.err = fmt.Errorf("csv header read error %w %q", err, string(hBytes))
				return
			}
			h = false
			fmt.Println("debug set header", header)
			continue
		}

		select {
		case <-ctx.Done():
			break loop
		case err := <-errChan:
			if skipErrors {
				ds.skipCount++
				log.Println(err)
			} else {
				ds.err = err
				break loop
			}
		default:
			dataIn <- scanner.Bytes()
		}
	}
	if scanner.Err() != nil {
		ds.err = scanner.Err()
	}
	files.Close() // close the reader
	sts := files.Stats()
	log.Printf("processed %d files at %s", sts.Files, sts.Path)

	close(dataIn)
	for {
		select {
		case e := <-errChan:
			if skipErrors {
				log.Println(e)
				ds.skipCount++
			} else {
				ds.err = e
			}
		default:
		}
		if i := atomic.LoadInt32(&activeThreads); i == 0 {
			break
		}
	}
	close(rowChan)
	close(errChan)
}

func NewDataSet(csv bool, delim rune) *DataSet {
	return &DataSet{
		dbSchema:   make([]DbColumn, 0),
		insertCols: make([]string, 0),
		csvData:    csv,
		delimiter:  delim,
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
			if k.Default == nil && !k.Nullable {
				var s string
				switch k.TypeName {
				case "int":
					s = "0"
				case "float":
					s = "0.0"
				}
				k.Default = &s
			}
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
		k.FieldKey = jKey
		meta = append(meta, k)
	}

	return meta, cols
}

// MakeCsvHeader creates a string slice based on the first row of the file
// I think we will have a problem if there are multiple files with header rows
func MakeCsvHeader(line []byte, delim rune) (header []string, err error) {
	reader := csv.NewReader(bytes.NewReader(line)) // push a csv record line into a csv reader to parse
	reader.Comma = delim
	header, err = reader.Read()
	if err != nil {
		err = fmt.Errorf("header csv read  %w", err)
	}
	return header, err
}

// MakeCsvRow creates a Row from byte slice data
func MakeCsvRow(dbSchema []DbColumn, line []byte, header []string, delim rune) (row Row, err error) {
	row = make(Row, len(dbSchema))
	reader := csv.NewReader(bytes.NewReader(line)) // push a csv record line into a csv reader to parse
	reader.Comma = delim

	r, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("csv read  %w", err)
	}

	for k, f := range dbSchema {
		var found bool // was the field found
		var idx int    // the index of the field

		// look for field in header
		for i, h := range header {
			found = false
			if h == f.FieldKey {
				found = true
				idx = i
				break
			}
		}

		// if the field was not found in the header set to default value
		if !found && !f.Nullable {
			if f.Default == nil {
				return nil, fmt.Errorf("%v is required", f.FieldKey)
			}
			row[k] = *f.Default
		} else if f.TypeName == "int" {
			s := strings.Split(r[idx], ".")
			row[k], err = strconv.ParseInt(s[0], 10, 64)
		} else if f.TypeName == "float" {
			row[k], err = strconv.ParseFloat(r[idx], 64)
		} else {
			row[k] = r[idx]
		}
	}

	return row, err
}

// MakeRow Takes the insert columns and the json byte data (jRow) and adds to the Dataset rows slice
// an error is returned if the row cannot be added to the DataSet rows
func MakeRow(dbSchema []DbColumn, j JsonData) (row Row, err error) {
	row = make(Row, len(dbSchema))
	for k, f := range dbSchema {
		v, found := j[f.FieldKey]
		if !found && !f.Nullable {
			if f.Default == nil {
				return nil, fmt.Errorf("%v is required", f.FieldKey)
			}
			j[f.FieldKey] = *f.Default
		}
		switch x := v.(type) {
		case string:
			if dbSchema[k].TypeName == "int" {
				j[f.FieldKey], err = strconv.ParseInt(x, 10, 64)
			}
			if dbSchema[k].TypeName == "float" {
				j[f.FieldKey], err = strconv.ParseFloat(x, 64)
			}
		case float64:
			// convert a float to an int if the schema is an int type
			if dbSchema[k].TypeName == "int" {
				if x != float64(int64(x)) {
					err = fmt.Errorf("add_row: cannot convert number value to int64 for %s value: %v type: %s",
						f.Name, v, dbSchema[k].DataType)
				}
				j[f.FieldKey] = int64(x)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("add row  %w", err)
		}
		row[k] = j[f.FieldKey]
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

	sort.Strings(s)
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

func CreateInserts(rowChan chan Row, queryChan chan string, tableName string, cols []string, batchSize int) {
	if batchSize < 0 {
		batchSize = 10000
	}

	fields := strings.Join(cols, ",")
	header := "insert into " + tableName + "(" + fields + ")\n" + "  VALUES \n"

	var f bytes.Buffer
	var rowCount int
	f.WriteString(header)
	for row := range rowChan {
		// create row
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
		rowCount++

		// check limit and reset buffer
		if rowCount >= batchSize {
			f.Truncate(f.Len() - 2)
			f.WriteString(";\n")
			queryChan <- f.String()
			f.Reset()
			f.WriteString(header)
			rowCount = 0
		}
	}

	// finish partial rows
	if rowCount > 0 {
		f.Truncate(f.Len() - 2)
		f.WriteString(";\n")
		queryChan <- f.String()
	}
	close(queryChan)
}
