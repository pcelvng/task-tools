package bootstrap

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
	btoml "gopkg.in/BurntSushi/toml.v0"
	ptoml "gopkg.in/pelletier/go-toml.v1"
)

type Runner interface {
	Run(context.Context) error
}

// NewTMApp will create a new taskmaster bootstrap application.
// *appName: defines the taskmaster name; acts as a name for identification and easy-of-use (required)
// *mkr: MakeWorker function that the launcher will call to create a new worker.
// *options: a struct pointer to additional specific application config options. Note that
//          the bootstrapped WorkerApp already provides bus and launcher config options and the user
//          can request to add postgres and mysql config options.
func NewTMApp(appName string, runner Runner, options Validator) *TMApp {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	if options == nil {
		options = &NilValidator{}
	}

	return &TMApp{
		appName: appName,
		tmOpt:   newTMOptions(appName),
		appOpt:  options,
		runner:  runner,
		lgr:     log.New(os.Stderr, "", log.LstdFlags),
	}
}

type TMApp struct {
	appName     string // application task type
	version     string // application version
	description string // info help string that show expected info format
	runner      Runner

	// options
	tmOpt     *tmOptions    // standard worker options (bus and launcher)
	appOpt    Validator     // extra WorkerApp options; should be pointer to a Validator struct
	pgOpts    *pgOptions    // postgres config options
	mysqlOpts *mysqlOptions // mysql config options
	fileOpts  *fileOptions

	lgr      *log.Logger // application logger instance
	mysql    *sql.DB     // mysql connection
	postgres *sql.DB     // postgres connection
}

// Start is non-blocking and will perform application startup
// tasks such as:
// *Parsing and handling flags
// *Parsing and validating the config file
// *Setting config defaults
//
// Note that start will handle application closure if there
// was an error during startup or a flag option was provided
// that asked the application to show the version, for example.
// So, if start is able to finish by returning, the user knows
// it is safe to move on.
func (a *TMApp) Initialize() {
	a.setHelpOutput() // add description to help

	// flags
	if !flag.Parsed() {
		flag.Parse()
	}
	a.handleFlags()

	// options
	//err := a.loadOptions()
	//if err != nil {
	//	a.logFatal(err)
	//}

	// validate WorkerApp options
	//err = a.appOpt.Validate()
	//if err != nil {
	//	a.logFatal(err)
	//}
}

func (a *TMApp) setHelpOutput() {
	// custom help screen
	flag.Usage = func() {
		if a.AppName() != "" {
			fmt.Fprintln(os.Stderr, a.AppName()+" worker")
			fmt.Fprintln(os.Stderr, "")
		}
		if a.description != "" {
			fmt.Fprintln(os.Stderr, a.description)
			fmt.Fprintln(os.Stderr, "")
		}
		fmt.Fprintln(os.Stderr, "Flag options:")
		flag.PrintDefaults()
	}
}

func (a *TMApp) logFatal(err error) {
	a.lgr.SetFlags(0)
	if a.AppName() != "" {
		a.lgr.SetPrefix(a.AppName() + ": ")
	} else {
		a.lgr.SetPrefix("")
	}
	a.lgr.Fatalln(err.Error())
}

func (a *TMApp) handleFlags() {
	// version
	if *showVersion || *v {
		a.showVersion()
	}

	// gen config (sent to stdout)
	if *genConfig || *g {
		a.genConfig()
	}

	// configPth required
	//if *configPth == "" && *c == "" {
	//	a.logFatal(errors.New("-config (-c) config file path required"))
	//}
}

func (a *TMApp) showVersion() {
	prefix := ""
	if a.AppName() != "" {
		prefix = a.AppName() + " "
	}
	if a.version == "" {
		fmt.Println(prefix + "version not specified")
	} else {
		fmt.Println(prefix + a.version)
	}
	os.Exit(0)
}

func (a *TMApp) genConfig() {
	var appOptB, wkrOptB, fileOptB, pgOptB, mysqlOptB []byte
	var err error

	// TMApp options
	appOptB, err = ptoml.Marshal(reflect.Indirect(reflect.ValueOf(a.appOpt)).Interface())
	if err != nil {
		a.lgr.SetFlags(0)
		if a.AppName() != "" {
			a.lgr.SetPrefix(a.AppName() + ": ")
		} else {
			a.lgr.SetPrefix("")
		}
		a.lgr.Fatalln(err.Error())
	}

	// tm standard options
	wkrOptB, err = ptoml.Marshal(*a.tmOpt)
	if err != nil {
		a.lgr.SetFlags(0)
		if a.AppName() != "" {
			a.lgr.SetPrefix(a.AppName() + ": ")
		} else {
			a.lgr.SetPrefix("")
		}
		a.lgr.Fatalln(err.Error())
	}

	// file options
	if a.fileOpts != nil {
		fileOptB, err = ptoml.Marshal(*a.fileOpts)
	}
	if err != nil {
		a.lgr.SetFlags(0)
		if a.AppName() != "" {
			a.lgr.SetPrefix(a.AppName() + ": ")
		} else {
			a.lgr.SetPrefix("")
		}
		a.lgr.Fatalln(err.Error())
	}

	// postgres options
	if a.pgOpts != nil {
		pgOptB, err = ptoml.Marshal(*a.pgOpts)
	}
	if err != nil {
		a.lgr.SetFlags(0)
		if a.AppName() != "" {
			a.lgr.SetPrefix(a.AppName() + ": ")
		} else {
			a.lgr.SetPrefix("")
		}
		a.lgr.Fatalln(err.Error())
	}

	// mysql options
	if a.mysqlOpts != nil {
		mysqlOptB, err = ptoml.Marshal(*a.mysqlOpts)
	}

	// err
	if err != nil {
		a.lgr.SetFlags(0)
		if a.AppName() != "" {
			a.lgr.SetPrefix(a.AppName() + ": ")
		} else {
			a.lgr.SetPrefix("")
		}
		a.lgr.Fatalln(err.Error())
	}

	fmt.Printf("# '%v' taskmaster options\n", a.AppName())
	fmt.Print(string(appOptB))
	fmt.Print(string(wkrOptB))
	fmt.Print(string(fileOptB))
	fmt.Print(string(pgOptB))
	fmt.Print(string(mysqlOptB))

	os.Exit(0)
}

func (a *TMApp) loadOptions() error {
	cpth := *configPth
	if *c != "" {
		cpth = *c
	}

	// WorkerApp options
	if _, err := btoml.DecodeFile(cpth, a.appOpt); err != nil {
		return err
	}

	// worker options
	if _, err := btoml.DecodeFile(cpth, a.tmOpt); err != nil {
		return err
	}

	// file options
	if a.fileOpts != nil {
		_, err := btoml.DecodeFile(cpth, a.fileOpts)
		if err != nil {
			return err
		}
	}

	// postgres options (if requested)
	if a.pgOpts != nil {
		_, err := btoml.DecodeFile(cpth, a.pgOpts)
		if err != nil {
			return err
		}

		// connect
		pg := a.pgOpts.Postgres
		a.postgres, err = db.Postgres(pg.Username, pg.Password, pg.Host, pg.DBName)
		if err != nil {
			return err
		}
	}

	// mysql options (if requested)
	if a.mysqlOpts != nil {
		_, err := btoml.DecodeFile(cpth, a.mysqlOpts)
		if err != nil {
			return err
		}

		// connect
		mysql := a.mysqlOpts.MySQL
		a.mysql, err = db.MySQL(mysql.Username, mysql.Password, mysql.Host, mysql.DBName)
		if err != nil {
			return err
		}
	}

	return nil
}

// Run will run until the application is complete
// and then exit.
func (a *TMApp) Run() {
	ctx, cncl := context.WithCancel(context.Background())
	a.Log("taskmaster running")
	done := make(chan error)
	go func() {
		done <- a.runner.Run(ctx)
	}()

	select {
	case <-sigChan:
		cncl()

		// wait up to x seconds and close
		tckr := time.NewTicker(time.Second * 2)
		select {
		case <-tckr.C:
		case <-done:
		}
	case err := <-done:
		if err != nil {
			a.logFatal(err)
		}
	}

	os.Exit(0)
}

// Version sets the application version. The version
// is what is shown if the '-version' flag is specified
// when running the WorkerApp.
func (a *TMApp) Version(version string) *TMApp {
	a.version = version
	return a
}

// Description allows the user to set a description of the
// worker that will be shown with the help screen.
//
// The description should also include information about
// what the worker expects from the NewWorker 'info' string.
func (a *TMApp) Description(description string) *TMApp {
	a.description = description
	return a
}

// FileOpts provides file options such as aws connection info.
func (a *TMApp) FileOpts() *TMApp {
	if a.fileOpts == nil {
		a.fileOpts = &fileOptions{}
		a.fileOpts.FileOpt.FileBufPrefix = a.AppName()
	}
	return a
}

// MySQLOpts will parse mysql db connection
// options from the config toml file.
//
// If using mysql options then a mysql db connection
// is made during startup. The mysql db connection is
// available through the MySQL() method.
//
// MySQLOpts needs to be called before Start() to be effective.
//
// If the user needs more than one db connection then those
// connection options need to be made available with the WorkerApp
// initialization. Note that the DBOptions struct is available
// to use in this way.
func (a *TMApp) MySQLOpts() *TMApp {
	if a.mysqlOpts == nil {
		a.mysqlOpts = &mysqlOptions{}
	}
	return a
}

// PostgresOpts will parse postgres db connection
// options from the config toml file.
//
// If using postgres options then a postgres db connection
// is made during startup. The postgres db connection is
// available through the Postgres() method.
//
// PostgresOpts needs to be called before Start() to be effective.
//
// If the user needs more than one db connection then those
// connection options need to be made available with the WorkerApp
// initialization. Note that the DBOptions struct is available
// to use in this way.
func (a *TMApp) PostgresOpts() *TMApp {
	if a.pgOpts == nil {
		a.pgOpts = &pgOptions{}
	}
	return a
}

func (a *TMApp) GetFileOpts() *file.Options {
	if a.fileOpts == nil {
		return nil
	}
	return &a.fileOpts.FileOpt
}

// SetLogger allows the user to override the default
// application logger (stdout) with a custom one.
//
// If the provided logger is nil the logger output is discarded.
//
// SetLogger should be called before initializing the application.
func (a *TMApp) SetLogger(lgr *log.Logger) *TMApp {
	if lgr != nil {
		a.lgr = lgr
	}

	return a
}

// AppName returns the AppName initialized with
// the WorkerApp.
func (a *TMApp) AppName() string {
	return a.appName
}

// MySQLDB returns the MySQL sql.DB application connection.
// Will be nil if called before Start() or MySQLOpts() was
// not called.
func (a *TMApp) MySQLDB() *sql.DB {
	return a.mysql
}

// PostgresDB returns the Postgres sql.DB application connection.
// Will be nil if called before Start() or PostgresOpts() was
// not called.
func (a *TMApp) PostgresDB() *sql.DB {
	return a.postgres
}

// Logger returns a reference to the application logger.
func (a *TMApp) Logger() *log.Logger {
	return a.lgr
}

// NewConsumer is a convenience method that will use
// the bus config information to create a new consumer
// instance. Can optionally provide a topic and channel
// on which to consume. All other bus options are the same.
func (a *TMApp) NewConsumer(topic, channel string) bus.Consumer {
	return newConsumer(*a.tmOpt.BusOpt, topic, channel)
}

// NewProducer will use the bus config information
// to create a new producer instance.
func (a *TMApp) NewProducer() bus.Producer {
	return newProducer(*a.tmOpt.BusOpt)
}

// Log is a wrapper around the application logger Printf method.
func (a *TMApp) Log(format string, v ...interface{}) {
	a.lgr.Printf(format, v...)
}

func newTMOptions(appName string) *tmOptions {
	bOpt := task.NewBusOptions("nsq") // nsq default for bootstrapping
	bOpt.InTopic = appName
	bOpt.InChannel = appName

	return &tmOptions{
		BusOpt:      bOpt,
		LauncherOpt: task.NewLauncherOptions(appName),
	}
}

// appOptions provides general options available to
// all workers.
type tmOptions struct {
	BusOpt      *bus.Options          `toml:"bus"`
	LauncherOpt *task.LauncherOptions `toml:"launcher"`
}
