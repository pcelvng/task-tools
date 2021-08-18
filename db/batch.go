package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os/user"
	"time"

	_ "github.com/apache/calcite-avatica-go/v5"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/pcelvng/task-tools/db/batch"
	"github.com/pkg/errors"
)

// BatchLoader implementations should have an initializer that
// also pings the db to check the connection.
type BatchLoader interface {
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

	// AddRow will add a row to the totals rows that will be prepared,
	// executed and committed when Commit is called. No validation is performed
	// when calling AddRow but if the len of any row provided to AddRow != len(cols)
	// then Commit will return an error without starting the transaction.
	// Other types of errors, such as problems with the row values will be detected
	// by the specific db server or by the underlying go adapter. Either way, such
	// errors will be detected and returned only after a call to Commit.
	AddRow(row []interface{})

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
	//
	// The order of cols is important and must match the order of row values when
	// calling AddRow.
	Commit(ctx context.Context, tableName string, cols ...string) (batch.Stats, error)
}

// Phoenix is a convenience initializer to obtain a Phoenix / Avatica connection
func Phoenix(server string, maxConns, maxIdleConns, maxConnLifeMins int) (*sql.DB, error) {
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

// MySQL is a convenience initializer to obtain a MySQL DB connection.
//
// Note that this connection has an option to set transaction isolation level to
// 'serializable' to enforce more true atomic batch loading.
func MySQL(un, pass, host, dbName string) (*sql.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&tx_isolation=serializable", un, pass, host, dbName)
	dbConn, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}

	// ping
	if err = dbConn.Ping(); err != nil {
		return nil, err
	}

	return dbConn, nil
}

// MySQLTx is a convenience initializer to obtain a MySQL DB connection.
//
// Note that this connection will set the default transaction isolation level to
// 'serializable' to enforce more true atomic batch loading.
func MySQLTx(un, pass, host, dbName string, serializable bool) (*sql.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", un, pass, host, dbName)
	if serializable {
		connStr += "&tx_isolation=serializable"
	}
	dbConn, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}

	// ping
	if err = dbConn.Ping(); err != nil {
		return nil, err
	}

	return dbConn, nil
}

// Postgres is a convenience initializer to obtain a Postgres DB connection.
//
// Note that this connection will set the default transaction isolation level to
// 'serializable' to enforce more true atomic batch loading.
func Postgres(un, pass, host, dbName string) (*sql.DB, error) {
	if dbName == "" {
		return nil, errors.New("postgres dbname is required")
	}

	if un == "" {
		// postgres user (default is current user)
		usr, _ := user.Current()
		un = usr.Username
	}

	//connStr := fmt.Sprintf("user=%s password=%s host=%s dbname=%s sslmode=disable", un, pass, host, dbName)
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&default_transaction_isolation=serializable", un, pass, host, dbName)
	dbConn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// ping
	if err = dbConn.Ping(); err != nil {
		return nil, err
	}

	return dbConn, nil
}

// PostgresTx is a convenience initializer to obtain a Postgres DB connection
//
// Note that this connection has an option to set transaction isolation level to
// 'serializable' to enforce more true atomic batch loading.
func PostgresTx(un, pass, host, dbName string, serializable bool) (*sql.DB, error) {
	if dbName == "" {
		return nil, errors.New("postgres dbname is required")
	}

	if un == "" {
		// postgres user (default is current user)
		usr, _ := user.Current()
		un = usr.Username
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", un, pass, host, dbName)
	if serializable {
		connStr += "&default_transaction_isolation=serializable"
	}
	dbConn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// ping
	if err = dbConn.Ping(); err != nil {
		return nil, err
	}

	return dbConn, nil
}

// NewBatchLoader will create a BatchLoader.
// dbType should be:
// * "postgres" for Postgres loading
// * "mysql" for MySQL loading
// * "nop" for using the nop batch loader "nop://", "nop://commit_err"
//
// Other adapters have not been tested but will likely work
// if they support transactions and the '?' execution placeholder
// value.
func NewBatchLoader(dbType string, sqlDB *sql.DB) BatchLoader {
	u, _ := url.Parse(dbType)
	scheme := u.Scheme
	if scheme == "nop" || dbType == "nop" {
		host := u.Host
		return batch.NewNopBatchLoader(host)
	}

	return batch.NewBatchLoader(dbType, sqlDB)
}
