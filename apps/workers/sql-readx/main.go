package main

import (
	"fmt"
	"log"

	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
)

const (
	taskType = "sql_readx"
	desc     = `extract data from a mysql or postgres table or execute a command

info params
 - origin: (alternative to query) - path to a file containing a sql statement
 - query: (instead of file) - statement to execute
 - exec: execute statement instead of running as a query
 - dest: (required for query) - file path to where the file should be written 
 - table: (required with field) - table (schema.table) to read from 
 - field: - map of columns of fields. 
	Query: list of columns to read from and the json field that should be used to write the values. 
	Exec: key to be replaced with value in statment. NOTE: key are wrapped with brackets {key} -> value

example 
{"task":"sql_readx","info":"?dest=./data.json&table=report.impressions&field=id:my_id|date:date"}
{"task":"sql_readx","info":"./query.sql?dest=./data.json"}
{"task":"sql_readx","info":"./query.sql?exec&field=date:2020-01-01"}
`
)

type options struct {
	DBOptions `toml:"sql"`

	FOpts *file.Options `toml:"file"`
	db    *sqlx.DB
}

type DBOptions struct {
	Type        string `toml:"type" commented:"true"`
	Username    string `toml:"username" commented:"true"`
	Password    string `toml:"password" commented:"true"`
	Host        string `toml:"host" comment:"host can be 'host:port', 'host', 'host:' or ':port'"`
	DBName      string `toml:"dbname"`
	SSLMode     string `toml:"sslmode" comment:"default is disable"`
	SSLCert     string `toml:"sslcert"`
	SSLKey      string `toml:"sslkey"`
	SSLRootCert string `toml:"sslrootcert"`
}

func (o *options) Validate() error {
	errs := appenderr.New()
	if o.Host == "" {
		errs.Addf("missing db host")
	}
	if o.DBName == "" {
		errs.Addf("missing db name")
	}
	return errs.ErrOrNil()
}

// connectDB creates a connection to the database
func (o *options) connectDB() (err error) {
	var driverName string
	var dsn string
	switch o.Type {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", o.Username, o.Password, o.Host, o.DBName)
		driverName = "mysql"
		o.db, err = sqlx.Connect(driverName, dsn)
	case "postgres":
		driverName = "postgres"
		if o.SSLMode == "" {
			o.db, err = db.PGx(o.Username, o.Password, o.Host, o.DBName)
		} else {
			o.db, err = db.PGxSSL(o.Username, o.Password, o.Host, o.DBName, o.SSLMode, o.SSLCert, o.SSLKey, o.SSLRootCert)
		}
	default:
		return fmt.Errorf("unknown db type %s", o.Type)
	}

	return err
}

func main() {
	opts := &options{
		FOpts: file.NewOptions(),
		DBOptions: DBOptions{
			Type:     "mysql",
			Username: "user",
			Password: "pass",
			Host:     "127.0.0.1:3306",
			DBName:   "db",
		},
	}
	app := bootstrap.NewWorkerApp(taskType, opts.NewWorker, opts).
		Description(desc).
		Version(tools.Version)

	app.Initialize()

	// setup database connection
	if err := opts.connectDB(); err != nil {
		log.Fatal("db connect: ", err)
	}

	app.Run()
}
