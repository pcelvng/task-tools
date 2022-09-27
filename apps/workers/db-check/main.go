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
	taskType    = "task.dbcheck"
	description = `
Runs record count checks against a given table's data to verify that the data is updated as expected

uri params:
db        - required database name to use for query connections
table     - required table name to validate
dt_column - required column name that holds the date value to query
date      - the date to compare data against tolerance level
offset    - if given, this is the offset duration back from date to compare tolerance
tolerance - (default 0.05) percent is the allowed percentage of deviation between offset and compare row counts
              0.05 (5%) means the difference between the row counts can vary by 5 percent and be accepted
              a greater difference would send an alert, if compare is not provided this check is not done

Example task 
(table.schema checked for records where my_date = 2022-01-02 and 2022-01-01 has 10% as many records as 2022-01-02):
  {"type":"task.dbcheck","info":"?db=mydbname&table=table.schema&dt_column=my_date&date=2022-01-02&offset=24h&tolerance=0.1"}`
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
	Psql map[string]Postgres `toml:"postgres"` // mulitple postgres db connections name:settings

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
		// connect to iap database (salesforce data)
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
