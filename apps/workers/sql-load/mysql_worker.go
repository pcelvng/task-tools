package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gtools "github.com/jbsmith7741/go-tools"
	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
)

type workerMySQL struct {
	options
	task.Meta

	Params InfoURI

	//flist    []string // list of full path file(s)
	fReader  file.Reader
	ds       *DataSet // the processing data for loading
	delQuery string   // query statement built from DeleteMap

	queryRunTime time.Duration // query running time
}

func (o *options) newMySQL(info string) task.Worker {
	w := &workerMySQL{
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

func (w *workerMySQL) DoTask(ctx context.Context) (task.Result, string) {
	// read the table schema to know the types for each column
	err := w.QuerySchema()
	if err != nil {
		return task.Failed(err)
	}
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	// read the files for loading, verify columns types

	rowChan := make(chan Row, 100)
	w.ds.dbSchema, w.ds.insertCols = PrepareMeta(w.ds.dbSchema, w.Params)

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

		w.queryRunTime = time.Now().Sub(start)
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
		w.queryRunTime = time.Now().Sub(start)
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
func (w *workerMySQL) QuerySchema() (err error) {
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

	w.ds.UpdateTypeName()

	return nil
}
