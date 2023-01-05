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
file_type: (default:json) a file path i.e., /folder/filename.csv will default to parse delimited data
delimiter: (default:,) the csv delimiter, default is a comma, you can use "tab" or "\t"
table: (required), the name of the table to be inserted into i.e., schema.table_name
delete : allows insert into pre-existing data by deleting previous data. 
    - provide a list of delete key:values to be used in the delete statement
    - "?delete=date:2020-07-01|id:7"
delete_sql : allows a place to provide custom where clause to build a custom delete query
    - provide a statement (after the where clause) that defines what to delete
		- should be url encoded for safety
		- for example unencoded: "t1 >= '2023-01-02T00:00:00' and t1 <= '2023-01-02T23:00:00' and id = 123456"
		- then url encoded: "?delete_sql=t1%20%3E%3D%20%272023-01-02T00%3A00%3A00%27%20and%20t1%20%3C%3D%20%272023-01-02T23%3A00%3A00%27%20and%20id%20%3D%20123456%20"
truncate: allows insert into pre-existing table by truncating before insertion
fields : allows mapping different json key values to different database column names
    - provide a list of field name mapping {DB column name}:{json key name} to be mapped 
    - ?fields=dbColumnName:jsonkey
cached_insert: improves insert times by caching data into a temp table
batch_size: (default:10000) number of rows to insert at a time (higher number increases memory usage) 
Example task:
 
{"type":"sql_load","info":"gs://bucket/path/to/file.json?table=schema.table_name&delete=date:2020-07-01|id:7"}
{"type":"sql_load","info":"gs://bucket/path/of/files/to/load/?table=schema.table_name"}
{"type":"sql_load","info":"gs://bucket/path/to/file.json?table=schema.table_name&delete=date:2020-07-01|id:7&fields=dbColumnName:jsonKeyValue"}

{"type":"sql_load","info":"gs://bucket/path/of/files/to/load/*.tsv?table=schema.table_name&file_type=csv&delimiter=tab"}`
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

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
		o := opts.Postgres
		if o.SSLMode == "" || o.SSLMode == "disable" {
			opts.sqlDB, err = db.Postgres(o.Username, o.Password, o.Host, o.DBName)
		} else {
			opts.sqlDB, err = db.PGSSL(o.Username, o.Password, o.Host, o.DBName, o.SSLMode, o.SSLCert, o.SSLKey, o.SSLRootcert)
		}
		if err != nil {
			opts.Postgres.Password = "secret"
			log.Fatalf("cannot connect to Postgres Instance %+v (%s)", opts.Postgres, err.Error())
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
