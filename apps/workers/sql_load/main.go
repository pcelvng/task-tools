package main

import (
	"database/sql"
	"errors"
	"log"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
)

type options struct {
	Postgres bootstrap.DBOptions `toml:"postgres"`
	MySQL    bootstrap.DBOptions `toml:"mysql"`

	sqlDB    *sql.DB
	producer bus.Producer
	fileOpts *file.Options
	dbDriver string // postgres, mysql - for the batchloader
}

var (
	taskType    = "sql_load"
	description = `sql_load app is a generic worker to load a file from local, gs or s3, with a format of newline delimited json, 
into a configured sql connection. 
Initially only postgresql will be supported, but later support can be added for mysql, etc...

info query params:
table_name : required, the table name should be given in the info string so the app knows where to attempt to insert the data.
delete : allows insert into  pre-existing data by deleting previous data. 
    - provide a list of delete key:values to be used in the delete statement
    - "?delete=date:2020-07-01|id:7"
fields : allows mapping different json key values to different database column names
    - provide a list of field name mapping {json key name}:{DB column name} to be mapped 
    - ?fields=jsonKey:dbColumnName

Example task:
 
{"type":"sql_load","info":"gs://bucket/path/to/file.json?table=schema.table_name&delete=date:2020-07-01|id:7"}
{"type":"sql_load","info":"gs://bucket/path/of/files/to/load/?table=schema.table_name"}
{"type":"sql_load","info":"gs://bucket/path/to/file.json?table=schema.table_name&delete=date:2020-07-01|id:7&fields=jsonKeyValue:dbColumnName"}`
)

func main() {
	var err error

	opts := &options{}
	app := bootstrap.NewWorkerApp(taskType, opts.newWorker, opts).
		Version(tools.String()).
		Description(description).
		FileOpts()

	app.Initialize()

	opts.producer = app.NewProducer()
	opts.fileOpts = app.GetFileOpts()

	if opts.MySQL.Host != "" {
		opts.dbDriver = "mysql"
		opts.sqlDB, err = db.MySQL(opts.MySQL.Username, opts.MySQL.Password, opts.MySQL.Host, opts.MySQL.DBName)
		if err != nil {
			log.Fatalf("cannot connect to MySQL Instance %+v", opts.MySQL)
		}
	}

	if opts.Postgres.Host != "" {
		opts.dbDriver = "postgres"
		opts.sqlDB, err = db.Postgres(opts.Postgres.Username, opts.Postgres.Password, opts.Postgres.Host, opts.Postgres.DBName)
		if err != nil {
			log.Fatalf("cannot connect to Postgres Instance %+v", opts.Postgres)
		}
	}

	app.Run()
}

func (o *options) Validate() error {
	if o.MySQL.Host == "" && o.Postgres.Host == "" {
		return errors.New("host is required for at least one DB connection (mysql or postgresql)")
	}
	return nil
}
