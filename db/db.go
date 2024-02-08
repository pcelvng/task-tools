package db

import (
	"database/sql"
	"errors"
	"fmt"
	"os/user"

	"github.com/jmoiron/sqlx"
)

type Options struct {
	Username     string `toml:"username" commented:"true"`
	Password     string `toml:"password" commented:"true"`
	Host         string `toml:"host" comment:"host can be 'host:port', 'host', 'host:' or ':port'"`
	DBName       string `toml:"dbname"`
	Serializable bool   `toml:"serializable" comment:"set isolation level to serializable, required for proper writing to database" commented:"true"`
	SSL          SSL    `toml:"SSL"`
}

type SSL struct {
	// Mode-PG: disable, allow, prefer, *require*, verify-ca, verify-full
	// Mode-MySQL: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
	Mode     string `toml:"mode" comment:"require is set if certs found"`
	Cert     string `toml:"cert"`
	Key      string `toml:"key"`
	Rootcert string `toml:"rootcert"`
}

// isSet checks that paths to cert files are provided
func (s SSL) isSet() bool {
	return s.Cert != "" && s.Key != "" && s.Rootcert != ""
}

func (o *Options) PG() (*sql.DB, error) {
	if o.SSL.isSet() {
		if o.SSL.Mode == "" {
			o.SSL.Mode = "require"
		}
		return PGSSL(o.Username, o.Password, o.Host, o.DBName, o.SSL.Mode, o.SSL.Cert, o.SSL.Key, o.SSL.Rootcert)
	}
	return PostgresTx(o.Username, o.Password, o.Host, o.DBName, o.Serializable)
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

	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?connect_timeout=5&sslmode=disable&default_transaction_isolation=serializable", un, pass, host, dbName)
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

// PGSSL is a convenience initializer to obtain a Postgres DB connection using ssl certs
//
// Note that this connection will set the default transaction isolation level to
// 'serializable' to enforce more true atomic batch loading.
func PGSSL(user, pass, host, dbName, sslMode, cert, key, caCert string) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?connect_timeout=5&default_transaction_isolation=serializable&sslmode=%s&sslcert=%s&sslkey=%s&sslrootcert=%s",
		user, pass, host, dbName, sslMode, cert, key, caCert)
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

func (o Options) PGx() (*sqlx.DB, error) {
	if o.SSL.isSet() {
		return PGxSSL(o.Username, o.Password, o.Host, o.DBName, o.SSL.Mode, o.SSL.Cert, o.SSL.Key, o.SSL.Rootcert)
	}
	return PGx(o.Username, o.Password, o.Host, o.DBName)
}

// PGx is a convenience initializer to obtain a Postgres sqlx.DB connection
//
// Note that this connection will set the default transaction isolation level to
// 'serializable' to enforce more true atomic batch loading.
func PGx(user, pass, host, dbName string) (*sqlx.DB, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?connect_timeout=5&default_transaction_isolation=serializable&sslmode=disable",
		user, pass, host, dbName)
	dbConn, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return dbConn, nil
}

// PGxSSL is a convenience initializer to obtain a Postgres sqlx.DB connection using ssl certs
//
// Note that this connection will set the default transaction isolation level to
// 'serializable' to enforce more true atomic batch loading.
func PGxSSL(user, pass, host, dbName, sslMode, cert, key, caCert string) (*sqlx.DB, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?connect_timeout=5&default_transaction_isolation=serializable&sslmode=%s&sslcert=%s&sslkey=%s&sslrootcert=%s",
		user, pass, host, dbName, sslMode, cert, key, caCert)
	dbConn, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return dbConn, nil
}

func (o Options) MySQL() (*sql.DB, error) {
	return MySQLTx(o.Username, o.Password, o.Host, o.DBName, o.Serializable)
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
