package main

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task/bus"
)

const (
	taskType    = "task.db-check"
	description = `
Runs checks against a given table's data to verify that the data is updated as expected

uri params:
db_src     - database source
table      - schema.table name to validate
type       - type of check
             * missing - alerts if 0 records are found for selected date
             * null - alerts if null values are found in selected field for selected date
             * zero - alerts if zero sum values are found for selected fields for selected date/times
                    - will also alert if no records are found for selected date/times
field      - field name being checked
date_field - date/timestamp field to query
date_type  - type of date_field ("dt" = date -or- "ts" = timestamp)
date       - date value to use in query
group_ts   - timestamp field to group by for hour level checking for tables with both a date and a timestamp field
             (optional "zero" type field used for efficiency purposes; date_field should be date type)

Example:
{"type":"task.db-check","info":"?db_src=mydbsrc&table=myschema.mytable&type=missing|null|zero&field=myfield&date_field=mydatefield&date_type=dt|ts&date=2023-05-24"}

!Note! "missing" type checks do not need the "field" param since the record count only relies on the "date_field" and date value`
)

type Postgres struct {
	Host        string `toml:"host"`
	User        string `toml:"username"`
	Pass        string `toml:"password"`
	DBName      string `toml:"dbname"`
	SSLMode     string `toml:"sslmode" comment:"default is disable, use require for ssl"`
	SSLCert     string `toml:"sslcert"`
	SSLKey      string `toml:"sslkey"`
	SSLRootcert string `toml:"sslrootcert"`

	DB *sqlx.DB
}

type options struct {
	Slack string `toml:"slack"`

	Bus  *bus.Options        `toml:"bus"`
	Psql map[string]Postgres `toml:"postgres"` // multiple postgres db server/source connections name:settings

	producer bus.Producer
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	o := &options{
		Psql: make(map[string]Postgres),
	}

	app := bootstrap.NewWorkerApp(taskType, o.newWorker, o).
		Version(tools.String()).
		Description(description).
		Initialize()

	o.producer = app.NewProducer()

	app.Run()
}

// database connection validation
func (o *options) Validate() (err error) {
	// check each psql connection
	for name, c := range o.Psql {
		if c.SSLMode == "" || c.SSLMode == "disable" {
			c.DB, err = db.PGx(c.User, c.Pass, c.Host, c.DBName)
			if err != nil {
				return fmt.Errorf("could not connect to postgres host:%s user:%s error:%s", c.Host, c.User, err.Error())
			}
		} else {
			c.DB, err = db.PGxSSL(c.User, c.Pass, c.Host, c.DBName, c.SSLMode, c.SSLCert, c.SSLKey, c.SSLRootcert)
			if err != nil {
				return fmt.Errorf("could not connect to postgres (ssl) host:%s user:%s error:%s", c.Host, c.User, err.Error())
			}
		}
		o.Psql[name] = c
	}
	return nil
}
