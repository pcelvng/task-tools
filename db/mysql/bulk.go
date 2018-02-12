package mysql

import (
	"context"
	"database/sql"

	"github.com/pcelvng/task-tools/db/stat"
)

var (
	insertTmpl = "INSERT INTO %s (%s) VALUES (%s);\n"
)

func NewBatchLoader(db *sql.DB) (*BatchLoader, error) {

	return &BatchLoader{
		db:   db,
		cols: make([]string, 0),
		rows: make([][]interface{}, 0),
	}, nil
}

// BatchLoader will:
// - accept records row-by-row to insert as a batch
// - remove records to be replaced or updated.
// - get a count of removed records
// - insert all rows at once
// - the entire batch operation is performed in a transaction
//   so the table updated at once.
//
// Transaction:
// 1. Begin Transaction
// 2. Count records to be removed
// 3. Remove old records (if any)
// 4. Insert new records
// 5. Commit
// 6. Rollback if there is a problem
//
// Need to know:
// - table to insert into
// - columns names and number of columns
// - column values
type BatchLoader struct {
	db       *sql.DB
	delQuery string
	delVals  []interface{}
	cols     []string        // column names - order must match each row value order.
	rows     [][]interface{} // row values - order must match provided column order.
}

func (l *BatchLoader) Delete(query string, vals ...interface{}) {
	l.delQuery = query
	l.delVals = vals
}

func (l *BatchLoader) Columns(cols []string) {
	l.cols = cols
}

func (l *BatchLoader) AddRow(row []interface{}) {
	l.rows = append(l.rows, row)
}

func (l *BatchLoader) Commit(ctx context.Context, tableName string) (stat.Stats, error) {

	return stat.Stats{}, nil
}
