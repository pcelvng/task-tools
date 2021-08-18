package main

import (
	"database/sql"
	"errors"
	"log"
	"math/rand"
	"time"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
)

type options struct {
	Postgres bootstrap.DBOptions `toml:"postgres"`
	MySQL    bootstrap.DBOptions `toml:"mysql"`
	Phoenix  bootstrap.DBOptions `toml:"phoenix"`

	sqlDB *sql.DB

	producer bus.Producer
	fileOpts *file.Options
	dbDriver string // postgres, mysql - for the batchloader
}

var (
	taskType    = "sql_load"
	description = `is a generic worker to load a newline delimited json into a sql databse. 
Initially only postgresql will be supported, but later support can be added for mysql, etc...

info query params:
table: (required), the name of the table to be inserted into i.e., schema.table_name
delete : allows insert into pre-existing data by deleting previous data. 
    - provide a list of delete key:values to be used in the delete statement
    - "?delete=date:2020-07-01|id:7"
truncate: allows insert into pre-existing table by truncating before insertion
fields : allows mapping different json key values to different database column names
    - provide a list of field name mapping {DB column name}:{json key name} to be mapped 
    - ?fields=dbColumnName:jsonkey
cached_insert: improves insert times by caching data into a temp table
batch_size: (default:10000) number of rows to insert at a time (higher number increases memory usage)
field_value: allows you to set a value for a specific field (column) name i.e., ?LOAD_DATE={timestamp}
Example task:
 
{"type":"sql_load","info":"gs://bucket/path/to/file.json?table=schema.table_name&delete=date:2020-07-01|id:7"}
{"type":"sql_load","info":"gs://bucket/path/of/files/to/load/?table=schema.table_name"}
{"type":"sql_load","info":"gs://bucket/path/to/file.json?table=schema.table_name&delete=date:2020-07-01|id:7&fields=dbColumnName:jsonKeyValue"}`
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func main() {
	var err error

	o := &options{}
	app := bootstrap.NewWorkerApp(taskType, o.newWorker, o).
		Version(tools.String()).
		Description(description).
		FileOpts()

	app.Initialize()

	o.producer = app.NewProducer()
	o.fileOpts = app.GetFileOpts()

	if o.Phoenix.Host != "" {
		o.dbDriver = "avatica"
		o.sqlDB, err = db.Phoenix(o.Phoenix.Host, o.Phoenix.MaxConns, o.Phoenix.MaxIdleConns, o.Phoenix.MaxConnLifeMins)
		if err != nil {
			log.Fatalf("cannot connect to phoenix / avatica instance %+v error:%s", o.Phoenix, err.Error())
		}

	}

	if o.MySQL.Host != "" {
		o.dbDriver = "mysql"
		o.sqlDB, err = db.MySQL(o.MySQL.Username, o.MySQL.Password, o.MySQL.Host, o.MySQL.DBName)
		if err != nil {
			log.Fatalf("cannot connect to MySQL Instance %+v error:%s", o.MySQL, err.Error())
		}
	}

	if o.Postgres.Host != "" {
		o.dbDriver = "postgres"
		o.sqlDB, err = db.Postgres(o.Postgres.Username, o.Postgres.Password, o.Postgres.Host, o.Postgres.DBName)
		if err != nil {
			log.Fatalf("cannot connect to Postgres Instance %+v error:%s", o.Postgres, err.Error())
		}
	}

	app.Run()
}

func (o *options) Validate() error {
	if o.MySQL.Host == "" && o.Postgres.Host == "" && o.Phoenix.Host == "" {
		return errors.New("host is required for at least one DB connection")
	}
	return nil
}
