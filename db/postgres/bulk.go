package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"sync"

	"github.com/pcelvng/task-tools/db/stat"
)

var maxBatchSize = 100 // max number of rows in a single insert statement

// NewBatchLoader will return an instance of a Postgres BatchLoader.
// The initializer will not verify that the underlying driver is
// Postgres. It is up to the user to make sure it is.
func NewBatchLoader(db *sql.DB) *BatchLoader {
	return &BatchLoader{
		db:           db,
		maxBatchSize: maxBatchSize,
		cols:         make([]string, 0),
		fRows:        make([]interface{}, 0),
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
	db           *sql.DB
	maxBatchSize int // maximum size of a batch
	delQuery     string
	delVals      []interface{}
	cols         []string      // column names - order must match each row value order.
	fRows        []interface{} // flattened row values for all rows

	mu sync.Mutex
}

func (l *BatchLoader) Delete(query string, vals ...interface{}) {
	l.delQuery = query
	l.delVals = vals
}

func (l *BatchLoader) Columns(cols []string) {
	l.cols = cols
}

func (l *BatchLoader) AddRow(row []interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.fRows = append(l.fRows, row...)
}

func (l *BatchLoader) Commit(ctx context.Context, tableName string) (stat.Stats, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	sts := stat.New()

	// must have cols defined
	if len(l.cols) == 0 {
		return sts, errors.New("columns not provided")
	}

	// check rows have correct number of values
	if len(l.fRows)%len(l.cols) > 0 {
		return sts, errors.New("rows values do not match number of columns")
	}

	// number of rows
	numRows := len(l.fRows) / len(l.cols)

	// batches info
	numBatches, batchSize, lastBatchSize := numBatches(l.maxBatchSize, numRows)

	// do transaction
	return l.doTx(numRows, numBatches, batchSize, lastBatchSize, tableName, ctx)
}

// doTx will execute the transaction.
func (l *BatchLoader) doTx(numRows, numBatches, batchSize, lastBatchSize int, tableName string, ctx context.Context) (stat.Stats, error) {
	sts := stat.New()

	// standard batch bulk insert
	insQ := GenInsert(l.cols, batchSize, tableName)

	// last batch bulk insert
	var lastInsQ string
	if lastBatchSize != batchSize {
		lastInsQ = GenInsert(l.cols, lastBatchSize, tableName)
	}

	// begin
	started := time.Now()
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return sts, err
	}

	// prepare ins stmt
	cx, _ := context.WithCancel(ctx)
	insStmt, err := tx.PrepareContext(cx, insQ)
	if err != nil {
		tx.Rollback()
		return sts, err
	}

	// prepare last ins stmt (if needed)
	lastStmt := insStmt
	if lastInsQ != "" {
		cx, _ = context.WithCancel(ctx)
		lastStmt, err = tx.PrepareContext(cx, lastInsQ)
		if err != nil {
			tx.Rollback()
			return sts, err
		}
	}

	// execute delete (if provided)
	if l.delQuery != "" {
		cx, _ = context.WithCancel(ctx)
		rslt, err := tx.ExecContext(cx, l.delQuery, l.delVals...)
		if err != nil {
			tx.Rollback()
			return sts, err
		}
		sts.Removed, _ = rslt.RowsAffected()
	}

	// execute inserts
	numCols := len(l.cols)
	numVals := batchSize * numCols // number of values in a standard batch
	for b := 0; b < numBatches; b++ {
		cx, _ = context.WithCancel(ctx)

		// handle last batch
		if b == (numBatches - 1) {
			insStmt = lastStmt
			numVals = lastBatchSize * numCols
		}

		// do insert
		rslt, err := insStmt.ExecContext(cx, l.fRows[b*numCols:b*numCols+numVals]...)
		if err != nil {
			tx.Rollback()
			return sts, err
		}
		insertCnt, _ := rslt.RowsAffected()
		sts.Inserted += insertCnt
	}
	ended := time.Now()

	// more stats
	sts.SetStarted(started)
	sts.Dur = stat.Duration{ended.Sub(started)}
	sts.Table = tableName
	sts.Rows = int64(numRows)
	sts.Cols = numCols

	return sts, nil
}

// numBatches will return the number of batches and the
// number of rows in the last batch.
// If batches = 1 then last will be the length of the first
// batch because first and last are the same.
func numBatches(maxBatchSize, numRows int) (batches int, batchSize, lastBatchSize int) {
	batches = numRows / batchSize
	lastBatchSize = numRows % batchSize
	if lastBatchSize > 0 {
		batches += 1
	}

	batchSize = maxBatchSize
	if batches == 1 {
		batchSize = lastBatchSize
	}
	return batches, batchSize, lastBatchSize
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
