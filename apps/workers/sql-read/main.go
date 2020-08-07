package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/jmoiron/sqlx"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
)

const (
	taskType = "sql_read"
	desc     = `extract data from a mysql or postgres table 

info params
 - origin: (alternative to field) - path to a file containing a sql query to extract the date  
 - dest: (required) - file path to where the file should be written 
 - table: (required with field) - table (schema.table) to read from 
 - field: (alternative to origin) - list of columns to read from and the json field that should be used to write the values. 

example 
{"task":"sql_read","info":"?dest=./data.json&table=report.impressions&field=id:my_id|date:date"}`
)

type options struct {
	DBOptions `toml:"mysql"`

	FOpts *file.Options `toml:"file"`
	db    *sqlx.DB
}

type DBOptions struct {
	Type     string `toml:"type" commented:"true"`
	Username string `toml:"username" commented:"true"`
	Password string `toml:"password" commented:"true"`
	Host     string `toml:"host" comment:"host can be 'host:port', 'host', 'host:' or ':port'"`
	DBName   string `toml:"dbname"`
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
	var dsn string
	switch o.Type {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", o.Username, o.Password, o.Host, o.DBName)
	case "postgres":
		host, port := o.Host, ""
		if v := strings.Split(o.Host, ":"); len(v) > 1 {
			host, port = v[0], v[1]
		}

		dsn = fmt.Sprintf("host=%s dbname=%s sslmode=disable", host, o.DBName)
		if o.Username != "" {
			dsn += " user=" + o.Username
		}
		if o.Password != "" {
			dsn += " password=" + o.Password
		}
		if port != "" {
			dsn += " port=" + port
		}
	default:
		return fmt.Errorf("unknown db type %s", o.Type)
	}
	o.db, err = sqlx.Open("mysql", dsn)
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
		log.Fatal("db connect", err)
	}

	app.Run()
}
