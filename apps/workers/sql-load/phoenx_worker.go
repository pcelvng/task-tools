package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
)

type workerPhoenix struct {
	options
	task.Meta

	Params InfoURI

	//flist    []string // list of full path file(s)
	fReader  file.Reader
	ds       *DataSet // the processing data for loading
	delQuery string   // query statement built from DeleteMap

	queryRunTime time.Duration // query running time
}

func (o *options) newPhoenix(info string) task.Worker {
	w := &workerPhoenix{
		options: *o,
		Meta:    task.NewMeta(),
		ds:      NewDataSet(),
	}

	if err := uri.Unmarshal(info, &w.Params); err != nil {
		return task.InvalidWorker("params uri.unmarshal: %v", err)
	}

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

func (w *workerPhoenix) DoTask(ctx context.Context) (task.Result, string) {
	var err error

	w.sqlDB, err = connectPhoenix(w.Phoenix.Host, 0, 0, 0)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer w.sqlDB.Close()

	err = w.QuerySchema()
	if err != nil {
		return task.Failed(fmt.Errorf("could not query table schema %w", err))
	}

	rowChan := make(chan Row, 100)
	w.ds.dbSchema, w.ds.insertCols = PrepareMeta(w.ds.dbSchema, w.Params)

	if len(w.ds.insertCols) == 0 {
		return task.Failed(fmt.Errorf("no table fields found for insert"))
	}

	go w.ds.ReadFiles(ctx, w.fReader, rowChan, w.Params.SkipErr)

	// run the delete / truncate query if provided
	var q string
	if w.delQuery != "" {
		q += w.delQuery + ";\n"
	} else if w.Params.Truncate {
		q += "delete from " + w.Params.Table + ";\n"
	}
	if q != "" {
		_, err = w.sqlDB.ExecContext(ctx, q)
		if err != nil {
			return task.Failf("delete / truncate error %v", err)
		}
	}

	sqlStmt := bytes.NewBuffer([]byte("UPSERT INTO " + w.Params.Table + "("))
	sqlVals := bytes.NewBuffer([]byte(" VALUES("))

	// build the sql statment
	for i, c := range w.ds.insertCols {
		sqlStmt.WriteString(c)
		sqlVals.WriteString("?")

		if i < len(w.ds.insertCols)-1 {
			sqlStmt.WriteString(",")
			sqlVals.WriteString(",")
		} else {
			sqlStmt.WriteString(")")
			sqlVals.WriteString(")")
		}
	}

	stmt := sqlStmt.String() + sqlVals.String()
	fmt.Println(stmt)

	if w.ds.err != nil {
		return task.Failed(w.ds.err)
	}

	start := time.Now()
	err = runLoad(ctx, w.sqlDB, rowChan, stmt, w.Params.BatchSize)
	if err != nil {
		return task.Failed(err)
	}

	w.queryRunTime = time.Since(start)

	w.SetMeta("insert_records", fmt.Sprintf("%d", w.ds.rowCount))
	w.SetMeta("query_run_time", fmt.Sprintf("%v", w.queryRunTime.String()))

	return task.Completed("database load completed %s table: %s records: %d",
		w.dbDriver, w.Params.Table, w.ds.rowCount)
}

func runLoad(ctx context.Context, db *sql.DB, rowChan chan Row, query string, limit int) error {
	var err error
	var wg sync.WaitGroup
	var s *sql.Stmt
	var start time.Time

	count := 0

	// process loop to batch rows to load into the database
	for {
		start = time.Now()
		// create new sql statement for the batch
		s, err = db.Prepare(query)
		if err != nil {
			return fmt.Errorf("sql db prepare error %w", err)
		}
		count = 0
		// if the rowChan finishes or the count == limit close the batch in it's own go routine
		for r := range rowChan {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			count++

			_, err = s.ExecContext(ctx, r...)
			if err != nil {
				return fmt.Errorf("query exec error %w", err)
			}

			if count == limit {
				break
			}
		}

		// there are no more rows to process
		if count == 0 {
			break
		}

		// run the close statement in a go routine so the next batch can be started
		wg.Add(1)
		go func(sq *sql.Stmt, started time.Time, c int) {
			defer wg.Done()
			e := sq.Close()
			if e != nil {
				log.Println("error on statement close", e)
				return
			}
			log.Println("batch time", time.Since(started), "row count", c)
		}(s, start, count)
		if err != nil {
			return err
		}

	}

	// wait for all batches to finish processing
	wg.Wait()

	return nil
}

// QuerySchema updates the table schema (dbSchema) information in the worker DataSet
func (w *workerPhoenix) QuerySchema() (err error) {
	q := `SELECT COLUMN_NAME, DATA_TYPE, NULLABLE 
FROM SYSTEM.CATALOG WHERE COLUMN_NAME IS NOT NULL `
	params := make([]interface{}, 0)

	if strings.Contains(w.Params.Table, ".") {
		tbl := strings.Split(w.Params.Table, ".")
		q += "AND TABLE_NAME = ? AND TABLE_SCHEM = ?"
		params = append(params, tbl[1], tbl[0])
	} else {
		q += "AND TABLE_NAME = ? AND TABLE_SCHEM is NULL"
		params = append(params, w.Params.Table)
	}
	q += " ORDER BY COLUMN_NAME"

	stmt, err := w.sqlDB.Prepare(q)
	if err != nil {
		return fmt.Errorf("sql db prepare error %w", err)
	}

	rows, err := stmt.Query(params...)
	if err != nil {
		return fmt.Errorf("statement query error %w", err)
	}

	c := DbColumn{}
	var isNullable int
	var dataType int

	for rows.Next() {
		err := rows.Scan(
			&c.Name,
			&dataType,
			&isNullable,
		)
		if err != nil {
			return fmt.Errorf("query schema scan error %w", err)
		}
		c.Nullable = isNullable > 0
		c.DataType = typeDef(dataType)
		w.ds.dbSchema = append(w.ds.dbSchema, c)
	}
	w.ds.UpdateTypeName()
	return err
}

func (w *workerPhoenix) Insert() {

}

// connect to a phoenix db, 0 values on max will set defaults 30, 5, 5
func connectPhoenix(server string, maxConns, maxIdleConns, maxConnLifeMins int) (*sql.DB, error) {
	if maxConns == 0 {
		maxConns = 30
	}
	if maxIdleConns == 0 {
		maxIdleConns = 5
	}
	if maxConnLifeMins == 0 {
		maxConnLifeMins = 5
	}

	db, err := sql.Open("avatica", server)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(time.Minute * time.Duration(maxConnLifeMins))

	// ping
	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, err
}

func typeDef(t int) string {
	switch t {
	case -5:
		return "bigint"
	case 4:
		return "int"
	case 12:
		return "varchar"
	case 93:
		return "timestamp"
	case 3:
		return "decimal"
	case -6:
		return "tinyint"
	default:
		return ""
	}
}
