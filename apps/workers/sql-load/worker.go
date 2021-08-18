package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
)

type InfoURI struct {
	FilePath     string            `uri:"origin"`                     // file path to load one file or a list of files in that path (not recursive)
	Table        string            `uri:"table" required:"true"`      // insert table name i.e., "schema.table_name"
	SkipErr      bool              `uri:"skip_err"`                   // if bad records are found they are skipped and logged instead of throwing an error
	DeleteMap    map[string]string `uri:"delete"`                     // map used to build the delete query statement
	FieldsMap    map[string]string `uri:"fields"`                     // map json key values to different db table field names
	Truncate     bool              `uri:"truncate"`                   // truncate the table rather than delete
	CachedInsert bool              `uri:"cached_insert"`              // this will attempt to load the query data though a temp table (postgres only)
	BatchSize    int               `uri:"batch_size" default:"10000"` // number of rows to insert at once (50000 seems a good number for phoenix)
	FieldVals    map[string]string `uri:"field_value"`                // used to set a static or function value for a field
}

type Jsondata map[string]interface{}
type Row []interface{} // each row to be inserted

type DataSet struct {
	dbSchema   []DbColumn // the database schema for each column
	insertCols []string   // the actual db column names, must match dbrows
	rowCount   int32
	skipCount  int

	err error

	//mux sync.Mutex // needed to thread add row
}

type DbColumn struct {
	Name        string  // DB column name
	DataType    string  // DB data type
	IsNullable  string  // DB YES or NO string values
	Default     *string // DB default function / value (only used if the field is null and not in the json object)
	TypeName    string  // string, int, float
	JsonKey     string  // matching json key name
	Nullable    bool    // bool value if column is nullable (true) or not (false)
	StaticValue string  // this is the static value when user provides a field_value ie LOAD_DATE={timestamp}
}

var json = jsoniter.ConfigFastest

// newWorker is called to determine the new worker type based on db type
func (o *options) newWorker(info string) task.Worker {
	switch o.dbDriver {
	case "postgres":
		return o.newPostgres(info)
	case "mysql":
		return o.newMySQL(info)
	case "phoenix", "avatica":
		return o.newPhoenix(info)
	}

	if o.dbDriver == "postgres" {
		return o.newPostgres(info)
	}

	return nil
}

// Shared methods between various workers

// ReadFiles uses a files list and file.Options to read files and process data into a Dataset
// it will build the cols and rows for each file
func (ds *DataSet) ReadFiles(ctx context.Context, files file.Reader, rowChan chan Row, skipErrors bool) {
	errChan := make(chan error, 20)
	dataIn := make(chan []byte, 20)
	var activeThreads int32
	for i := 0; i < 20; i++ {
		activeThreads++
		go func() { // unmarshaler
			defer func() { atomic.AddInt32(&activeThreads, -1) }()
			for b := range dataIn {
				var j Jsondata
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
		}()
	}

	// read the lines of the file
	scanner := file.NewScanner(files)
loop:
	for scanner.Scan() {
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

func NewDataSet() *DataSet {
	return &DataSet{
		dbSchema:   make([]DbColumn, 0),
		insertCols: make([]string, 0),
	}
}

// PrepareMeta will check the dataset DBColumns and the mapping data to make sure
// all fields are accounted for, if it cannot find a db col in the jRow
// it will set that missing jRow value to nil if it's nullable in the db
// it will also check the json jRow key values against the cols list
func PrepareMeta(dbSchema []DbColumn, params InfoURI) (meta []DbColumn, cols []string) {
	// for the json record, add the json data keys
	// but only where the column was found in the database schema
	for _, k := range dbSchema {
		jKey := k.Name
		if v := params.FieldsMap[k.Name]; v != "" {
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

		// set the static field value if field_value is found in the table column list
		if v, ok := params.FieldVals[k.Name]; ok {
			k.StaticValue = v
			if strings.Contains(v, strings.ToLower("{timestamp}")) {
				v = time.Now().Format("2006-01-02 15:04:05")
				k.StaticValue = v
			}
		}

		// skip columns that have functions associated with them and no static value
		if k.Default != nil && strings.Contains(*k.Default, "(") && k.StaticValue == "" &&
			strings.Contains(*k.Default, ")") {
			continue
		}

		cols = append(cols, k.Name) // db column names
		k.JsonKey = jKey
		meta = append(meta, k)
	}

	return meta, cols
}

// MakeRow Takes the insert columns and the json byte data (jRow) and adds to the Dataset rows slice
// an error is returned if the row cannot be added to the DataSet rows
func MakeRow(dbSchema []DbColumn, j Jsondata) (row Row, err error) {
	row = make(Row, len(dbSchema))
	for k, f := range dbSchema {
		// if a static value is given, set the json field value to that static value
		if f.StaticValue != "" {
			j[f.JsonKey] = f.StaticValue
		}

		// if the field was not found in the json object and the field is not nullable
		v, found := j[f.JsonKey]
		if !found && !f.Nullable {
			if f.Default == nil {
				return nil, fmt.Errorf("%v is required", f.JsonKey)
			}
			j[f.JsonKey] = *f.Default
		}

		if dbSchema[k].StaticValue == "" {
			switch x := v.(type) {
			case string:
				if dbSchema[k].TypeName == "int" {
					j[f.JsonKey], err = strconv.ParseInt(x, 10, 64)
				}
				if dbSchema[k].TypeName == "float" {
					j[f.JsonKey], err = strconv.ParseFloat(x, 64)
				}
				// special format case for phoenix timestamp
				if dbSchema[k].DataType == "timestamp" {
					t, err := time.Parse(time.RFC3339, x)
					if err != nil {
						return nil, fmt.Errorf("cannot parse column %s timestamp %s", f.JsonKey, x)
					}
					if !t.IsZero() {
						j[f.JsonKey] = t.Format("2006-01-02 15:04:05")
					}
				}
			case float64:
				// convert a float to an int if the schema is an int type
				if dbSchema[k].TypeName == "int" {
					if x != float64(int64(x)) {
						err = fmt.Errorf("add_row: cannot convert number value to int64 for %s value: %v type: %s",
							f.Name, v, dbSchema[k].DataType)
					}
					j[f.JsonKey] = int64(x)
				}
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

func (ds *DataSet) UpdateTypeName() {
	for idx, c := range ds.dbSchema {
		if c.TypeName == "" {
			if strings.Contains(c.DataType, "char") || strings.Contains(c.DataType, "text") {
				ds.dbSchema[idx].TypeName = "string"
			}

			if strings.Contains(c.DataType, "int") || strings.Contains(c.DataType, "serial") {
				ds.dbSchema[idx].TypeName = "int"
			}

			if strings.Contains(c.DataType, "numeric") || strings.Contains(c.DataType, "dec") ||
				strings.Contains(c.DataType, "double") || strings.Contains(c.DataType, "real") ||
				strings.Contains(c.DataType, "fixed") || strings.Contains(c.DataType, "float") {
				ds.dbSchema[idx].TypeName = "float"
			}
		}
	}
}
