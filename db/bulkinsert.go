package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	pq "github.com/lib/pq"

	"github.com/pcelvng/task-tools/db/stat"
	"database/sql"
)

// BulkInserter implementations should have an initializer that
// also pings the db to check the connection.
type BulkInserter interface {
	// Delete takes a fully formed delete query string that
	// will be executed in the transaction before the inserts.
	Delete(query string)

	// AddRow should prepare the insert query but not actually send
	// the insert to the db until Commit is called.
	//
	// The reported Stats.InsertCnt will reflect the number of times
	// this method is called. The implementation will not attempt to
	// do a select count of the inserted records after insert or tally
	// the insert count as reported by the db.
	//
	// Even through the row column values and tableName could change
	// from call to call, if the user does so, the batch insert will
	// most likely fail since the underlying implementation will likely
	// perform multi-line inserts with each insert statment.
	// It is up to the user to make sure row map keys and values
	// are consistent across all AddRow calls.
	AddRow(r map[string]interface{}, tableName string)

	// Commit will execute the delete query and all inserts as efficiently
	// as the underlying adapter will allow. If there is a problem executing
	// or ctx is cancelled then the transaction should rollback.
	//
	// In the presence of a delete query the stat.Stats will do its best to
	// populate the number of rows deleted if possible from the underlying adapter.
	Commit(ctx context.Context) (stat.Stats, error)
}

type Options struct {
	Username string
	Password string
	Host string // host:port
	DBName string
}

func NewBulkInserter(dbType string, opt Options) (*SQLBatchLoader, error) {


}

// SQLBatchLoader will:
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
type SQLBatchLoader struct {
	db        *sqlx.DB
	tx        *sqlx.Tx
	fields    string
	fieldCnt  int
	tableName string
	dbSts     stat.Stats
	fields []string
	rows      [][]interface{}
}

type Record struct {
	Field1 string `db:"field1"`
	Field2 string
}

func Ping(db *sqlx.DB) error {
	if db == nil {
		return nil
	}

	// ping
	err = db.Ping()
	if err != nil {
		return err
	}
}

func (l *SQLBatchLoader) AddRow(r map[string]interface{}) error {
	columns := make([]string, len(r))
	values := make([]interface{}, len(r))
	placeholders := make([]string, len(r))
	i := 0
	for k, v := range r {
		columns[i] = k
		values[i] = v
		placeholders[i] = "?"
		i++
	}

	// generate '?' notation query (without insert values)
	insQ := fmt.Sprintf(
		`INSERT INTO table_name (%s) VALUES (%s);`,
		strings.Join(columns, ","), // join column
		// names
		strings.Join(placeholders, ","), // join "?"
	)

	// convert '?' standard notation into adapter specific notation
	insQ = l.db.Rebind(insQ)

	//stmt, err := l.db.Prepare(insQ)
	stmt, err := l.db.Preparex(insQ)
	sqlx.LoadFile()
}

func (l *SQLBatchLoader) CommitTx(ctx context.Context) error {
	tx, err := l.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	fieldNames := strings.Join(l.fields, ",")
	fieldNameValues := strings.Join(l.fields, ",:")
	tmpl := fmt.Sprintf(`INSERT INTO %s (%s) values (:%s)`, l.tableName, fieldNames, fieldNameValues)
	//stmt, err := l.tx.PrepareNamed(tmpl)

	l.tx.Rebind()
	for _, r := range l.rows {
		result, err := l.tx.NamedExec(tmpl, r)
		result.
	}
	//tx.Prepare(l.inserts)
	return nil
}

func (l *SQLBatchLoader) Stats() stat.Stats {
	return l.dbSts.Clone()
}
