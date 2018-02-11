package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"

	"github.com/pcelvng/task-tools/db/stat"
)

// BulkInserter implementations should have an initializer that
// also pings the db to check the connection.
type BulkInserter interface {
	// Delete takes a delete query string with optional vals values
	// and will be executed in the transaction before bulk inserts.
	// The delete will be rolled back if there was a problem anywhere
	// during the transaction.
	//
	// The delete statement will not be executed until Commit is
	// called.
	//
	// If query does not end with a ';' to end the statement then
	// a semicolon will be added. (necessary?)
	Delete(query string, vals ...interface{})

	// Columns is required before calling Commit. If Columns has
	// not been called then a call to Commit will return an error.
	// cols represents the table column names. The order of cols
	// is important and must match the order of row values when
	// calling AddRow.
	Columns(cols []string)

	// AddRow will add a row to the totals rows that will be prepared,
	// executed and committed when Commit is called. No validation is performed
	// when calling AddRow but if the len of any row provided to AddRow != len(cols)
	// then Commit will return an error without starting the transaction.
	// Other types of errors, such as problems with the row values will be detected
	// by the specific db server or by the underlying go adapter. Either way, such
	// errors will be detected and returned only after a call to Commit.
	AddRow(r []interface{})

	// Commit will execute the delete query and efficiently insert all rows. The
	// delete and inserts will all occur in a single transaction. If there is
	// a problem during the transaction then the transaction will be rolled back.
	//
	// In the presence of a delete query the stat.Stats will do its best to
	// populate the number of rows deleted from the underlying adapter.
	//
	// Cancelling ctx will cancel the transaction and rollback. A cancelled context
	// will result in Commit returning a non-nil error.
	//
	// Calling Commit more than once is allowed and will repeat the entire transaction.
	Commit(ctx context.Context, tableName string) (stat.Stats, error)
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
