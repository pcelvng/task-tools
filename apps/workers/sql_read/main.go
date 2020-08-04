package main

import (
	"fmt"
	"log"

	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/jmoiron/sqlx"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
)

const (
	taskType = "sql_read"
	desc     = ``
)

type options struct {
	DBOptions `toml:"mysql"`

	FOpts *file.Options
	db    *sqlx.DB
}

type DBOptions struct {
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
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", o.Username, o.Password, o.Host, o.DBName)
	o.db, err = sqlx.Open("mysql", dsn)
	return err
}

func main() {
	opts := &options{
		FOpts: file.NewOptions(),
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
