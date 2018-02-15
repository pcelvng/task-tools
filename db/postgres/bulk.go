package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pcelvng/task-tools/db/stat"
)

var maxBatchSize = 200 // max number of rows in a single insert statement

// NewBatchLoader will return an instance of a Postgres BatchLoader.
// The initializer will not verify that the underlying driver is
// Postgres. It is up to the user to make sure it is.
func NewBatchLoader(sqlDB *sql.DB) *BatchLoader {
	return &BatchLoader{
		sqlDB:        sqlDB,
		maxBatchSize: maxBatchSize,
		cols:         make([]string, 0),
		fRows:        make([]interface{}, 0),
	}
}

// BatchLoader will:
// - accept records row-by-row to insert as a batch
// - remove records to be replaced or updated.
// - get a count of removed records
// - insert all rows in efficient bulk batches
// - the entire batch operation is performed in a transaction
//   so the table is updated atomically and can be rolled back
//   if there is a problem.
//
// Transaction:
// 1. Begin Transaction
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
	sqlDB        *sql.DB
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

func (l *BatchLoader) AddRow(row []interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.fRows = append(l.fRows, row...)
}

func (l *BatchLoader) Commit(ctx context.Context, tableName string, cols ...string) (stat.Stats, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	sts := stat.New()
	l.cols = cols

	// must have cols defined
	if len(l.cols) == 0 {
		return sts, errors.New("columns not provided")
	}

	// number of rows
	numRows := len(l.fRows) / len(l.cols)

	// batches info
	numBatches, batchSize, lastBatchSize := numBatches(l.maxBatchSize, numRows)

	// do transaction
	return l.doTx(ctx, numRows, numBatches, batchSize, lastBatchSize, tableName)
}

// doTx will execute the transaction.
func (l *BatchLoader) doTx(ctx context.Context, numRows, numBatches, batchSize, lastBatchSize int, tableName string) (stat.Stats, error) {
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
	tx, err := l.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return sts, err
	}

	// prepare ins stmt
	insStmt, err := tx.PrepareContext(ctx, insQ)
	if err != nil {
		tx.Rollback()
		return sts, err
	}

	// prepare last ins stmt (if needed)
	var lastStmt *sql.Stmt
	if lastInsQ != "" {
		lastStmt, _ = tx.PrepareContext(ctx, lastInsQ)
	}
	if lastStmt == nil {
		lastStmt = insStmt
	}

	// execute delete (if provided)
	if l.delQuery != "" {
		rslt, err := tx.ExecContext(ctx, l.delQuery, l.delVals...)
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
		// handle last batch
		if b == (numBatches - 1) {
			insStmt = lastStmt
			numVals = lastBatchSize * numCols
		}

		// do insert
		rslt, err := insStmt.ExecContext(ctx, l.fRows[b*numCols:b*numCols+numVals]...)
		if err != nil {
			tx.Rollback()
			return sts, err
		}
		insertCnt, _ := rslt.RowsAffected()
		sts.Inserted += insertCnt
	}

	// commit
	err = tx.Commit()
	ended := time.Now()

	// more stats
	sts.SetStarted(started)
	sts.Dur = stat.Duration{ended.Sub(started)}
	sts.Table = tableName
	sts.Rows = int64(numRows)
	sts.Cols = numCols

	return sts, err
}

// numBatches will return the number of batches and the
// number of rows in the last batch.
// If batches = 1 then last will be the length of the first
// batch because first and last are the same.
func numBatches(maxBatchSize, numRows int) (batches int, batchSize, lastBatchSize int) {
	// will not allow divide by zero panic
	if maxBatchSize == 0 {
		return 0, 0, 0
	}

	// calc number of batches and the remainder.
	// if there is a remainder then that also counts
	// as a batch.
	batches = numRows / maxBatchSize
	lastBatchSize = numRows % maxBatchSize
	if lastBatchSize > 0 {
		batches += 1
	}

	// if there is just one batch
	// then the batchSize and the
	// lastBatchSize are the same. If
	// lastBatchSize is also zero then
	// there is one even batch equal to
	// the maxBatchSize.
	batchSize = maxBatchSize
	if batches == 1 && lastBatchSize != 0 {
		batchSize = lastBatchSize
	}

	// lastBatchSize == 0 when the number of rows is
	// evenly divisible by maxBatchSize.
	// In this case the lastBatchSize is the maxBatchSize
	// which is also the batchSize.
	if lastBatchSize == 0 {
		lastBatchSize = batchSize
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
