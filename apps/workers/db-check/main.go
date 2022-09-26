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
table      - required table name to validate
date_field - required field name that holds the date value
date1      - 1st date value to check for records
date2      - 2nd date to compare row counts between date1 and date2
offset     - if date2 is not given, this is the offset duration back from date1 to get date2
percent    - (default 0.05) percent is the allowed percentage of deviation between offset and compare row counts
              0.05 (5%) means the difference between the row counts can vary by 5 percent and be accepted
							a greater difference would send an alert, if compare is not provided this check is not done

Example task 
(affiliate.publishers table has records from 24h ago, the difference between 24h and 48h row counts should not vary more than 5%): 
  {"type":"task.dbcheck","info":"db_name?table=affiliate.publishers&compare=48h&percent=0.05"}`
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

	Bus  *bus.Options `toml:"bus"`
	Psql Postgres     `toml:"postgres"` // postgres login values

	producer bus.Producer
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	opts := &options{}

	app := bootstrap.NewWorkerApp(taskType, opts.newWorker, opts).
		Version(tools.String()).
		Description(description).
		Initialize()

	opts.producer = app.NewProducer()

	app.Run()
}

// database connection validation
func (o *options) Validate() (err error) {
	// connect to iap database (salesforce data)
	if o.Psql.SSLMode == "" || o.Psql.SSLMode == "disable" {
		o.Psql.DB, err = db.PGx(o.Psql.User, o.Psql.Pass, o.Psql.Host, o.Psql.DBName)
		if err != nil {
			return fmt.Errorf("could not connect to postgres host:%s user:%s error:%s", o.Psql.Host, o.Psql.User, err.Error())
		}
	} else {
		o.Psql.DB, err = db.PGxSSL(o.Psql.User, o.Psql.Pass, o.Psql.Host, o.Psql.DBName, o.Psql.SSLMode, o.Psql.SSLCert, o.Psql.SSLKey, o.Psql.SSLRootcert)
		if err != nil {
			return fmt.Errorf("could not connect to postgres (ssl) host:%s user:%s error:%s", o.Psql.Host, o.Psql.User, err.Error())
		}
	}
	return nil
}
