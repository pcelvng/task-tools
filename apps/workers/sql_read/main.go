package main

import (
	"github.com/jbsmith7741/go-tools/appenderr"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
)

const (
	taskType = "dbSync"
	desc     = ``
)

type options struct {
	DBOptions `toml:"mysql"`

	FOpts *file.Options
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

func main() {
	opts := &options{
		FOpts: file.NewOptions(),
	}
	app := bootstrap.NewWorkerApp(taskType, opts.NewWorker, opts).
		Description(desc).
		Version(tools.Version)

	app.Initialize()

	app.Run()
}
