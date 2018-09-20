package bootstrap

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"syscall"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
	btoml "gopkg.in/BurntSushi/toml.v0"
	ptoml "gopkg.in/pelletier/go-toml.v1"
)

type NewRunner func(*TaskMaster) Runner

type Runner interface {
	Run(ctx context.Context) error
	Info() interface{}
}

// NewTaskMaster will create a new taskmaster bootstrap application.
// *appName: defines the taskmaster name; acts as a name for identification and easy-of-use (required)
// *mkr: MakeWorker function that the launcher will call to create a new worker.
// *options: a struct pointer to additional specific application config options. Note that
//          the bootstrapped WorkerApp already provides bus and launcher config options and the user
//          can request to add postgres and mysql config options.
func NewTaskMaster(appName string, initFn NewRunner, options Validator) *TaskMaster {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	if options == nil {
		options = &NilValidator{}
	}

	return &TaskMaster{
		appName:    appName,
		tmOpt:      newTMOptions(appName),
		appOpt:     options,
		newRunner:  initFn,
		lgr:        log.New(os.Stderr, "", log.LstdFlags),
		statusPort: &statsOptions{HttpPort: 11000},
	}
}

func (tm *TaskMaster) AppOpt() interface{} {
	return tm.appOpt
}

type TaskMaster struct {
	appName     string // application task type
	version     string // application version
	description string // info help string that show expected info format
	newRunner   NewRunner
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

	statusPort *statsOptions // health status options (currently http port for requests)
}

// Initialize is non-blocking and will perform application startup
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
func (tm *TaskMaster) Initialize() *TaskMaster {
	tm.setHelpOutput() // add description to help

	// flags
	if !flag.Parsed() {
		flag.Parse()
	}
	tm.handleFlags()

	// options
	err := tm.loadOptions()
	if err != nil {
		tm.logFatal(err)
	}

	// validate WorkerApp options
	err = tm.appOpt.Validate()
	if err != nil {
		tm.logFatal(err)
	}
	tm.runner = tm.newRunner(tm)
	return tm
}

func (tm *TaskMaster) setHelpOutput() {
	// custom help screen
	flag.Usage = func() {
		if tm.AppName() != "" {
			fmt.Fprintln(os.Stderr, tm.AppName()+" worker")
			fmt.Fprintln(os.Stderr, "")
		}
		if tm.description != "" {
			fmt.Fprintln(os.Stderr, tm.description)
			fmt.Fprintln(os.Stderr, "")
		}
		fmt.Fprintln(os.Stderr, "Flag options:")
		flag.PrintDefaults()
	}
}

func (tm *TaskMaster) logFatal(err error) {
	tm.lgr.SetFlags(0)
	if tm.AppName() != "" {
		tm.lgr.SetPrefix(tm.AppName() + ": ")
	} else {
		tm.lgr.SetPrefix("")
	}
	tm.lgr.Fatalln(err.Error())
}

func (tm *TaskMaster) handleFlags() {
	// version
	if *showVersion || *ver {
		tm.showVersion()
	}

	// gen config (sent to stdout)
	if *genConfig || *g {
		tm.genConfig()
	}

	// configPth required
	if *configPth == "" && *c == "" {
		tm.logFatal(errors.New("-config (-c) config file path required"))
	}
}

func (tm *TaskMaster) showVersion() {
	prefix := ""
	if tm.AppName() != "" {
		prefix = tm.AppName() + " "
	}
	if tm.version == "" {
		fmt.Println(prefix + "version not specified")
	} else {
		fmt.Println(prefix + tm.version)
	}
	os.Exit(0)
}

func (tm *TaskMaster) genConfig() {
	var appOptB, wkrOptB, fileOptB, pgOptB, mysqlOptB, statsOptB []byte
	var err error

	// TaskMaster options
	appOptB, err = ptoml.Marshal(reflect.Indirect(reflect.ValueOf(tm.appOpt)).Interface())
	if err != nil {
		tm.lgr.SetFlags(0)
		if tm.AppName() != "" {
			tm.lgr.SetPrefix(tm.AppName() + ": ")
		} else {
			tm.lgr.SetPrefix("")
		}
		tm.lgr.Fatalln(err.Error())
	}

	// tm standard options
	wkrOptB, err = ptoml.Marshal(*tm.tmOpt)
	if err != nil {
		tm.lgr.SetFlags(0)
		if tm.AppName() != "" {
			tm.lgr.SetPrefix(tm.AppName() + ": ")
		} else {
			tm.lgr.SetPrefix("")
		}
		tm.lgr.Fatalln(err.Error())
	}

	// file options
	if tm.fileOpts != nil {
		fileOptB, err = ptoml.Marshal(*tm.fileOpts)
	}
	if err != nil {
		tm.lgr.SetFlags(0)
		if tm.AppName() != "" {
			tm.lgr.SetPrefix(tm.AppName() + ": ")
		} else {
			tm.lgr.SetPrefix("")
		}
		tm.lgr.Fatalln(err.Error())
	}

	// postgres options
	if tm.pgOpts != nil {
		pgOptB, err = ptoml.Marshal(*tm.pgOpts)
	}
	if err != nil {
		tm.lgr.SetFlags(0)
		if tm.AppName() != "" {
			tm.lgr.SetPrefix(tm.AppName() + ": ")
		} else {
			tm.lgr.SetPrefix("")
		}
		tm.lgr.Fatalln(err.Error())
	}

	// mysql options
	if tm.mysqlOpts != nil {
		mysqlOptB, err = ptoml.Marshal(*tm.mysqlOpts)
	}

	if tm.statusPort != nil {
		statsOptB, _ = ptoml.Marshal(*tm.statusPort)
	}

	// err
	if err != nil {
		tm.lgr.SetFlags(0)
		if tm.AppName() != "" {
			tm.lgr.SetPrefix(tm.AppName() + ": ")
		} else {
			tm.lgr.SetPrefix("")
		}
		tm.lgr.Fatalln(err.Error())
	}

	fmt.Printf("# '%v' taskmaster options\n", tm.AppName())
	fmt.Print(string(appOptB))
	fmt.Print(string(statsOptB))
	fmt.Print(string(wkrOptB))
	fmt.Print(string(fileOptB))
	fmt.Print(string(pgOptB))
	fmt.Print(string(mysqlOptB))

	os.Exit(0)
}

func (tm *TaskMaster) loadOptions() error {
	cpth := *configPth
	if *c != "" {
		cpth = *c
	}

	// status options
	if _, err := btoml.DecodeFile(cpth, tm.statusPort); err != nil {
		return err
	}

	// WorkerApp options
	if _, err := btoml.DecodeFile(cpth, tm.appOpt); err != nil {
		return err
	}

	// worker options
	if _, err := btoml.DecodeFile(cpth, tm.tmOpt); err != nil {
		return err
	}

	// file options
	if tm.fileOpts != nil {
		_, err := btoml.DecodeFile(cpth, tm.fileOpts)
		if err != nil {
			return err
		}
	}

	// postgres options (if requested)
	if tm.pgOpts != nil {
		_, err := btoml.DecodeFile(cpth, tm.pgOpts)
		if err != nil {
			return err
		}

		// connect
		pg := tm.pgOpts.Postgres
		tm.postgres, err = db.Postgres(pg.Username, pg.Password, pg.Host, pg.DBName)
		if err != nil {
			return err
		}
	}

	// mysql options (if requested)
	if tm.mysqlOpts != nil {
		_, err := btoml.DecodeFile(cpth, tm.mysqlOpts)
		if err != nil {
			return err
		}

		// connect
		mysql := tm.mysqlOpts.MySQL
		tm.mysql, err = db.MySQL(mysql.Username, mysql.Password, mysql.Host, mysql.DBName)
		if err != nil {
			return err
		}
	}

	return nil
}

// HandleRequest is a simple http handler function that takes the compiled status functions
// that are called and the results marshaled to return as the body of the response
func (tm *TaskMaster) HandleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	b, err := json.MarshalIndent(tm.runner.Info(), "", "  ")
	if b != nil && err == nil {
		// Replace the first { in the json string with the { + application name
		b = bytes.Replace(b, []byte(`{`), []byte(`{\n  "app_name":"`+tm.appName+`",`), 1)
	}
	w.Write(b)
}

// HttpPort gets the application http port for requesting
// a heath check on the application itself.
func (tm *TaskMaster) HttpPort() int {
	return tm.statusPort.HttpPort
}

// Start will run the http server on the provided handler port
func (tm *TaskMaster) start() {
	if tm.HttpPort() == 0 {
		log.Printf("http status server has been disabled")
		return
	}
	log.Printf("starting http status server on port %d", tm.HttpPort())

	http.HandleFunc("/", tm.HandleRequest)
	go func() {
		err := http.ListenAndServe(":"+strconv.Itoa(tm.HttpPort()), nil)
		log.Fatal("http health service failed", err)
	}()
}

// Run until the application is complete and then exit.
func (tm *TaskMaster) Run() {
	// Start the http health status service
	tm.start()
	ctx, cancel := context.WithCancel(context.Background())
	// do tasks
	go func() {
		select {
		case <-sigChan:
			cancel()
		}
	}()

	tm.Log("starting %v", tm.appName)
	if err := tm.runner.Run(ctx); err != nil {
		log.Fatal(err)
	}

	os.Exit(0)
}

// Version sets the application version. The version
// is what is shown if the '-version' flag is specified
// when running the WorkerApp.
func (tm *TaskMaster) Version(version string) *TaskMaster {
	tm.version = version
	return tm
}

// Description allows the user to set a description of the
// worker that will be shown with the help screen.
//
// The description should also include information about
// what the worker expects from the NewWorker 'info' string.
func (tm *TaskMaster) Description(description string) *TaskMaster {
	tm.description = description
	return tm
}

// FileOpts provides file options such as aws connection info.
func (tm *TaskMaster) FileOpts() *TaskMaster {
	if tm.fileOpts == nil {
		tm.fileOpts = &fileOptions{}
		tm.fileOpts.FileOpt.FileBufPrefix = tm.AppName()
	}
	return tm
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
func (tm *TaskMaster) MySQLOpts() *TaskMaster {
	if tm.mysqlOpts == nil {
		tm.mysqlOpts = &mysqlOptions{}
	}
	return tm
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
func (tm *TaskMaster) PostgresOpts() *TaskMaster {
	if tm.pgOpts == nil {
		tm.pgOpts = &pgOptions{}
	}
	return tm
}

func (tm *TaskMaster) GetFileOpts() *file.Options {
	if tm.fileOpts == nil {
		return nil
	}
	return &tm.fileOpts.FileOpt
}

// SetLogger allows the user to override the default
// application logger (stdout) with a custom one.
//
// If the provided logger is nil the logger output is discarded.
//
// SetLogger should be called before initializing the application.
func (tm *TaskMaster) SetLogger(lgr *log.Logger) *TaskMaster {
	if lgr != nil {
		tm.lgr = lgr
	}

	return tm
}

// AppName returns the AppName initialized with
// the WorkerApp.
func (tm *TaskMaster) AppName() string {
	return tm.appName
}

// MySQLDB returns the MySQL sql.DB application connection.
// Will be nil if called before Start() or MySQLOpts() was
// not called.
func (tm *TaskMaster) MySQLDB() *sql.DB {
	return tm.mysql
}

// PostgresDB returns the Postgres sql.DB application connection.
// Will be nil if called before Start() or PostgresOpts() was
// not called.
func (tm *TaskMaster) PostgresDB() *sql.DB {
	return tm.postgres
}

// Logger returns a reference to the application logger.
func (tm *TaskMaster) Logger() *log.Logger {
	return tm.lgr
}

// NewConsumer is a convenience method that will use
// the bus config information to create a new consumer
// instance. Can optionally provide a topic and channel
// on which to consume. All other bus options are the same.
func (tm *TaskMaster) NewConsumer() bus.Consumer {
	var busOpt bus.Options
	if tm != nil {
		busOpt = *tm.tmOpt.BusOpt
	}
	return newConsumer(busOpt, busOpt.InTopic, busOpt.InChannel)
}

// NewProducer will use the bus config information
// to create a new producer instance.
func (tm *TaskMaster) NewProducer() bus.Producer {
	var busOpt bus.Options
	if tm != nil {
		busOpt = *tm.tmOpt.BusOpt
	}
	return newProducer(busOpt)
}

// Log is a wrapper around the application logger Printf method.
func (tm *TaskMaster) Log(format string, v ...interface{}) {
	tm.lgr.Printf(format, v...)
}

func newTMOptions(appName string) *tmOptions {
	bOpt := task.NewBusOptions("nsq") // nsq default for bootstrapping
	bOpt.InTopic = appName
	bOpt.InChannel = appName

	return &tmOptions{
		BusOpt: bOpt,
	}
}

// appOptions provides general options available to
// all workers.
type tmOptions struct {
	BusOpt *bus.Options `toml:"bus"`
}
