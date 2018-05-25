package bootstrap

import (
	"database/sql"
	"errors"
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

var (
	sigChan     = make(chan os.Signal, 1) // signal handling
	configPth   = flag.String("config", "", "application config toml file")
	c           = flag.String("c", "", "alias to -config")
	showVersion = flag.Bool("version", false, "show app version and build info")
	ver         = flag.Bool("v", false, "alias to -version")
	genConfig   = flag.Bool("gen-config", false, "generate a config toml file to stdout")
	g           = flag.Bool("g", false, "alias to -gen-config")
)

// NewWorkerApp will create a new worker bootstrap application.
// *tskType: defines the worker type; the type of tasks the worker is expecting. Also acts as a name for identification (required)
// *mkr: MakeWorker function that the launcher will call to create a new worker.
// *options: a struct pointer to additional specific application config options. Note that
//          the bootstrapped WorkerApp already provides bus and launcher config options and the user
//          can request to add postgres and mysql config options.
func NewWorkerApp(tskType string, newWkr task.NewWorker, options Validator) *WorkerApp {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	if options == nil {
		options = &NilValidator{}
	}

	return &WorkerApp{
		tskType: tskType,
		newWkr:  newWkr,
		wkrOpt:  newWkrOptions(tskType),
		appOpt:  options,
		lgr:     log.New(os.Stderr, "", log.LstdFlags),
	}
}

type WorkerApp struct {
	tskType     string         // application task type
	version     string         // application version
	description string         // info help string that show expected info format
	newWkr      task.NewWorker // application MakeWorker function
	l           *task.Launcher

	// options
	wkrOpt    *wkrOptions   // standard worker options (bus and launcher)
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
func (a *WorkerApp) Initialize() {
	a.setHelpOutput() // add description to help

	// flags
	a.handleFlags()

	// validate WorkerApp options
	err := a.appOpt.Validate()
	if err != nil {
		a.logFatal(err)
	}

	// launcher
	a.l, err = task.NewLauncher(a.newWkr, a.wkrOpt.LauncherOpt, a.wkrOpt.BusOpt)
	if err != nil {
		a.logFatal(err)
	}
}

func (a *WorkerApp) setHelpOutput() {
	// custom help screen
	flag.Usage = func() {
		if a.TaskType() != "" {
			fmt.Fprintln(os.Stderr, a.TaskType()+" worker")
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

func (a *WorkerApp) logFatal(err error) {
	a.lgr.SetFlags(0)
	if a.TaskType() != "" {
		a.lgr.SetPrefix(a.TaskType() + ": ")
	} else {
		a.lgr.SetPrefix("")
	}
	a.lgr.Fatalln(err.Error())
}

func (a *WorkerApp) handleFlags() {
	if !flag.Parsed() {
		flag.Parse()
	}

	// version
	if *showVersion || *ver {
		a.showVersion()
	}

	// gen config (sent to stdout)
	if *genConfig || *g {
		a.genConfig()
	}

	var path string
	// configPth required
	if *configPth == "" && *c == "" {
		a.logFatal(errors.New("-config (-c) config file path required"))
	} else if *configPth != "" {
		path = *configPth
	} else {
		path = *c
	}

	// options

	err := a.loadOptions(path)
	if err != nil {
		a.logFatal(err)
	}
}

func (a *WorkerApp) showVersion() {
	prefix := ""
	if a.TaskType() != "" {
		prefix = a.TaskType() + " "
	}
	if a.version == "" {
		fmt.Println(prefix + "version not specified")
	} else {
		fmt.Println(prefix + a.version)
	}
	os.Exit(0)
}

func (a *WorkerApp) genConfig() {
	var appOptB, wkrOptB, fileOptB, pgOptB, mysqlOptB []byte
	var err error

	// WorkerApp options
	appOptB, err = ptoml.Marshal(reflect.Indirect(reflect.ValueOf(a.appOpt)).Interface())
	if err != nil {
		a.lgr.SetFlags(0)
		if a.TaskType() != "" {
			a.lgr.SetPrefix(a.TaskType() + ": ")
		} else {
			a.lgr.SetPrefix("")
		}
		a.lgr.Fatalln(err.Error())
	}

	// worker options
	wkrOptB, err = ptoml.Marshal(*a.wkrOpt)
	if err != nil {
		a.lgr.SetFlags(0)
		if a.TaskType() != "" {
			a.lgr.SetPrefix(a.TaskType() + ": ")
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
		if a.TaskType() != "" {
			a.lgr.SetPrefix(a.TaskType() + ": ")
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
		if a.TaskType() != "" {
			a.lgr.SetPrefix(a.TaskType() + ": ")
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
		if a.TaskType() != "" {
			a.lgr.SetPrefix(a.TaskType() + ": ")
		} else {
			a.lgr.SetPrefix("")
		}
		a.lgr.Fatalln(err.Error())
	}

	fmt.Printf("# '%v' worker options\n", a.TaskType())
	fmt.Print(string(appOptB))
	fmt.Print(string(wkrOptB))
	fmt.Print(string(fileOptB))
	fmt.Print(string(pgOptB))
	fmt.Print(string(mysqlOptB))

	os.Exit(0)
}

func (a *WorkerApp) loadOptions(cpth string) error {

	// WorkerApp options
	if _, err := btoml.DecodeFile(cpth, a.appOpt); err != nil {
		return err
	}

	// worker options
	if _, err := btoml.DecodeFile(cpth, a.wkrOpt); err != nil {
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
func (a *WorkerApp) Run() {
	// do tasks
	done, cncl := a.l.DoTasks()
	a.Log("listening for %s tasks on '%s'", a.wkrOpt.BusOpt.Bus, a.wkrOpt.BusOpt.InTopic)

	select {
	case <-sigChan:
		cncl()
		<-done.Done()
	case <-done.Done():
	}

	os.Exit(0)
}

// Version sets the application version. The version
// is what is shown if the '-version' flag is specified
// when running the WorkerApp.
func (a *WorkerApp) Version(version string) *WorkerApp {
	a.version = version
	return a
}

// Description allows the user to set a description of the
// worker that will be shown with the help screen.
//
// The description should also include information about
// what the worker expects from the NewWorker 'info' string.
func (a *WorkerApp) Description(description string) *WorkerApp {
	a.description = description
	return a
}

// FileOpts provides file options such as aws connection info.
func (a *WorkerApp) FileOpts() *WorkerApp {
	if a.fileOpts == nil {
		a.fileOpts = &fileOptions{}
		a.fileOpts.FileOpt.FileBufPrefix = a.TaskType()
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
func (a *WorkerApp) MySQLOpts() *WorkerApp {
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
func (a *WorkerApp) PostgresOpts() *WorkerApp {
	if a.pgOpts == nil {
		a.pgOpts = &pgOptions{}
	}
	return a
}

func (a *WorkerApp) GetFileOpts() *file.Options {
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
func (a *WorkerApp) SetLogger(lgr *log.Logger) *WorkerApp {
	if lgr != nil {
		a.lgr = lgr
	}

	return a
}

// TaskType returns the TaskType initialized with
// the WorkerApp.
func (a *WorkerApp) TaskType() string {
	return a.tskType
}

// MySQLDB returns the MySQL sql.DB application connection.
// Will be nil if called before Start() or MySQLOpts() was
// not called.
func (a *WorkerApp) MySQLDB() *sql.DB {
	return a.mysql
}

// PostgresDB returns the Postgres sql.DB application connection.
// Will be nil if called before Start() or PostgresOpts() was
// not called.
func (a *WorkerApp) PostgresDB() *sql.DB {
	return a.postgres
}

// Logger returns a reference to the application logger.
func (a *WorkerApp) Logger() *log.Logger {
	return a.lgr
}

// NewConsumer is a convenience method that will use
// the bus config information to create a new consumer
// instance. Can optionally provide a topic and channel
// on which to consume. All other bus options are the same.
func (a *WorkerApp) NewConsumer(topic, channel string) bus.Consumer {
	busOpt := bus.NewOptions(a.wkrOpt.BusOpt.Bus)
	busOpt.InBus = a.wkrOpt.BusOpt.InBus
	busOpt.InTopic = a.wkrOpt.BusOpt.InTopic
	busOpt.InChannel = a.wkrOpt.BusOpt.InChannel
	busOpt.LookupdHosts = a.wkrOpt.BusOpt.LookupdHosts
	busOpt.NSQdHosts = a.wkrOpt.BusOpt.NSQdHosts

	if topic != "" {
		busOpt.InTopic = topic
	}

	if channel != "" {
		busOpt.InChannel = channel
	}

	consumer, _ := bus.NewConsumer(busOpt)
	return consumer
}

// NewProducer will use the bus config information
// to create a new producer instance.
func (a *WorkerApp) NewProducer() bus.Producer {
	busOpt := bus.NewOptions(a.wkrOpt.BusOpt.Bus)
	busOpt.OutBus = a.wkrOpt.BusOpt.OutBus
	busOpt.LookupdHosts = a.wkrOpt.BusOpt.LookupdHosts
	busOpt.NSQdHosts = a.wkrOpt.BusOpt.NSQdHosts

	producer, _ := bus.NewProducer(a.wkrOpt.BusOpt)
	return producer
}

// Log is a wrapper around the application logger Printf method.
func (a *WorkerApp) Log(format string, v ...interface{}) {
	a.lgr.Printf(format, v...)
}

// Validator provides a standard
// method for running underlying validation
// for underlying object values.
type Validator interface {
	Validate() error
}

// NilValidator satisfies the
// Validator interface but does
// nothing.
type NilValidator struct{}

func (v *NilValidator) Validate() error {
	return nil
}

func newWkrOptions(tskType string) *wkrOptions {
	bOpt := task.NewBusOptions("nsq") // nsq default for bootstrapping
	bOpt.InTopic = tskType
	bOpt.InChannel = tskType

	return &wkrOptions{
		BusOpt:      bOpt,
		LauncherOpt: task.NewLauncherOptions(tskType),
	}
}

// appOptions provides general options available to
// all workers.
type wkrOptions struct {
	BusOpt      *bus.Options          `toml:"bus"`
	LauncherOpt *task.LauncherOptions `toml:"launcher"`
}

// fileOptions are only included at the request of the user.
// If added they are made available with the application file
// options object which can be accessed from the WorkerApp object.
type fileOptions struct {
	FileOpt file.Options `toml:"file"`
}

// mysqlOptions are only added at the request of the user.
// If they are added then the bootstrap WorkerApp will automatically
// attempt to connect to mysql.
type mysqlOptions struct {
	MySQL DBOptions `toml:"mysql"`
}

// postgresOptions are only added at the request of the user.
// If they are added then the bootstrap WorkerApp will automatically
// attempt to connect to postgres.
type pgOptions struct {
	Postgres DBOptions `toml:"postgres"`
}

type DBOptions struct {
	Username string `toml:"username" commented:"true"`
	Password string `toml:"password" commented:"true"`
	Host     string `toml:"host" comment:"host can be 'host:port', 'host', 'host:' or ':port'"`
	DBName   string `toml:"dbname"`
}

// Duration is a wrapper around time.Duration
// and allows for automatic toml string parsing of
// time.Duration values. Use this type in a
// custom config for automatic serializing and
// de-serializing of time.Duration.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d *Duration) MarshalTOML() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}
