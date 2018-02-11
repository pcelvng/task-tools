package mysql

import (
	"fmt"
	"strings"
	"database/sql"

	"github.com/lib/pq"
	"github.com/jmoiron/sqlx"

	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/db/stat"
)

/*
`
START TRANSACTION;

INSERT INTO table_name (column1, column2, column3,...)
values (value1, value2, value3,...);

INSERT INTO table_name (column1, column2, column3,...)
values (value1, value2, value3,...);

INSERT INTO table_name (column1, column2, column3,...)
values (value1, value2, value3,...);

INSERT INTO table_name (column1, column2, column3,...)
values (value1, value2, value3,...);

END TRANSACTION;
`
*/

var (
	insertTmpl = "INSERT INTO %s (%s) VALUES (%s);\n"
)

func NewBulkInserter(dsn string) (db.BulkInserter, error) {

	db, _ := sql.Open(driverName, dataSourceName)
	pq.Array()


	if db == nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return &SQLBatchLoader{
		db:    db,
		dbSts: stat.New(),
	}, nil
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
type BatchInsert struct {
	db        *sql.DB
	tx        *sql.Tx
	fields    string
	fieldCnt  int
	tableName string
	dbSts     stat.Stats
	rows      []map[string]interface{}
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
