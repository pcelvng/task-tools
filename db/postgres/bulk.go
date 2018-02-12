package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pcelvng/task-tools/db/stat"
)

var maxBatchSize = 100 // max number of rows in a single insert statement

// NewBatchLoader will return an instance of a Postgres BatchLoader.
// The initializer will not verify that the underlying driver is
// Postgres. It is up to the user to make sure it is.
func NewBatchLoader(db *sql.DB) *BatchLoader {
	return &BatchLoader{
		db:        db,
		batchSize: maxBatchSize,
		cols:      make([]string, 0),
		fRows:     make([][]interface{}, 0),
	}
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
	db        *sql.DB
	batchSize int // maximum size of a batch
	delQuery  string
	delVals   []interface{}
	cols      []string        // column names - order must match each row value order.
	fRows     [][]interface{} // flatten/batched row values - order must match provided column order.
}

func (l *BatchLoader) Delete(query string, vals ...interface{}) {
	l.delQuery = query
	l.delVals = vals
}

func (l *BatchLoader) Columns(cols []string) {
	l.cols = cols
}

func (l *BatchLoader) AddRow(row []interface{}) {
	l.fRows = append(l.fRows, row)
}

func (l *BatchLoader) Commit(ctx context.Context, tableName string) (stat.Stats, error) {
	// num of batches
	batches, lastBatchSize := numBatches(maxBatchSize, len(l.fRows))

	// batch sizes
	batchSize := maxBatchSize
	if batches == 1 {
		batchSize = lastBatchSize
	}

	// standard batch bulk insert
	ins := GenInsert(l.cols, batchSize, tableName)

	// last batch bulk insert
	lastIns := GenInsert(l.cols, lastBatchSize, tableName)

	// do transaction
	return l.doTx(batches, ins, lastIns, ctx)
}

// doTx will execute the transaction.
func (l *BatchLoader) doTx(batchCnt int, ins, lastIns string, ctx context.Context) (stat.Stats, error) {
	sts := stat.New()

	// begin
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return sts, nil
	}

	// prepare ins stmt
	cx, _ := context.WithCancel(ctx)
	insStmt, err := tx.PrepareContext(cx, ins)
	if err != nil {
		return sts, nil
	}

	// prepare last ins stmt (if needed)
	var lstStmt *sql.Tx
	if lastIns != "" {
		cx, _ = context.WithCancel(ctx)
		lstStmt, err := tx.PrepareContext(cx, lastIns)
		if err != nil {
			return sts, nil
		}
	}

	// execute delete (if provided)
	if l.delQuery != "" {
		cx, _ = context.WithCancel(ctx)
		rslt, err := tx.ExecContext(cx, l.delQuery, l.delVals...)
		if err != nil {
			return sts, nil
		}
		sts.RemovedCnt, _ = rslt.RowsAffected()
	}

	// execute inserts
	for b := 0; b < batchCnt-1; b++ {
		cx, _ = context.WithCancel(ctx)

		rslt, err := insStmt.ExecContext(cx, l.rows...)
		if err != nil {
			return sts, nil
		}
		sts.RemovedCnt, _ = rslt.RowsAffected()
	}
}

// flattenRows will flatten the rows so
// that each new 'row' will contains all the columns
// of a single batch.
func flattenRows(batches, lastBatchSize, numFRows int, rows [][]interface{}) [][]interface{} {
	fRows := make([][]interface{}, batches)
}

// numBatches will return the number of batches and the
// number of rows in the last batch.
// If batches = 1 then last will be the length of the first
// batch because first and last are the same.
func numBatches(batchSize, numRows int) (batches int, last int) {
	batches = numRows / batchSize
	last = numRows % batchSize
	if last > 0 {
		batches += 1
	}
	return batches, last
}

// GenBatchInsert will generate a Postgres parsable
// batch insert string with the specified '$' values
// for a multi-column insert statement of 'cols' columns
// and 'numRows' number of rows.
func GenInsert(cols []string, numRows int, tableName string) string {
	// all three values required for a correctly formed
	// insert statement.
	if len(cols) == 0 || numRows <= 0 || tableName == "" {
		return ""
	}

	rows := make([]string, numRows)

	// gen params
	params := genParams(len(cols) * numRows)

	// gen rows
	lCols := len(cols)
	for r := 0; r < numRows; r++ {
		rows[r] = strings.Join(params[r*lCols:r*lCols+lCols], ",")
	}

	// format final insert query
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s);",
		tableName,
		strings.Join(cols, ","),
		strings.Join(rows, "),("),
	)
}

// genParams provides a simple string slice with Postgres
// query params. If numParams == 3 then the result string
// slice values would be:
// {"$1","$2","$3"}
func genParams(numParams int) []string {
	params := make([]string, numParams)
	for i := 0; i < numParams; i++ {
		params[i] = "$" + strconv.Itoa(i+1)
	}
	return params
}
