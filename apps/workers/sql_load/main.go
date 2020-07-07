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
	FileTopic string              `toml:"file_topic" commented:"true" comment:"topic to publish written file stats, ignored when empty string"` // topic to publish information about written files
	Postgres  bootstrap.DBOptions `toml:"postgres"`
	MySQL     bootstrap.DBOptions `toml:"mysql"`

	sqlDB    *sql.DB
	producer bus.Producer
	fileOpts *file.Options
}

var (
	taskType    = "sql_load"
	description = `sql_load app is a simple worker to load a file from local, gs or s3, with a format of newline delimited json, 
into a configured sql connection. 
Initially only postgresql will be supported, but later support can be added for mysql, etc...

info query params:
table_name : required, the table name should be given in the info string so the app knows where to attempt to insert the data.
strict     : default false, all field names in the json string have to match the table field names exactly or an error is returned,
             when this is false extra field names in the json string are ignored. 
copy       : default false, when true transactions are not used, and the insert statement is built as a copy statement (postgresql only)

Example task:
 
{"type":"sql_load","info":"gs://bucket/path/to/file.json?table=schema.table_name"}`
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
		opts.sqlDB, err = db.MySQL(opts.MySQL.Username, opts.MySQL.Password, opts.MySQL.Host, opts.MySQL.DBName)
		if err != nil {
			log.Fatalf("cannot connect to MySQL Instance %+v", opts.MySQL)
		}
	}

	if opts.Postgres.Host != "" {
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
