package bootstrap

import (
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
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/bus/info"
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

type Worker struct {
	tskType     string         // application task type
	version     string         // application version
	description string         // info help string that show expected info format
	newWkr      task.NewWorker // application MakeWorker function

	l *task.Launcher
	c bus.Consumer
	p bus.Producer

	// options
	wkrOpt    *wkrOptions   // standard worker options (bus and launcher)
	appOpt    Validator     // extra Worker options; should be pointer to a Validator struct
	pgOpts    *pgOptions    // postgres config options
	mysqlOpts *mysqlOptions // mysql config options
	fileOpts  *fileOptions

	lgr      *log.Logger // application logger instance
	mysql    *sql.DB     // mysql connection
	postgres *sql.DB     // postgres connection

	statusPort *statsOptions // health status options (currently http port for requests)

	Info // info stats on various worker types
}

type Info struct {
	LauncherStats task.LauncherStats `json:"launcher"`
	ProducerStats info.Producer      `json:"producer"`
	ConsumerStats info.Consumer      `json:"consumer"`
}

// HandleRequest is a simple http handler function that takes the compiled status functions
// that are called and the results marshaled to return as the body of the response
func (app *Worker) HandleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	if app.c != nil {
		app.ConsumerStats = app.c.Info()
	}
	if app.l != nil {
		app.LauncherStats = app.l.Stats()
	}
	if app.p != nil {
		app.ProducerStats = app.p.Info()
	}

	b, _ := json.Marshal(&app.Info)

	w.Write(b)
}

// Start will run the http server on the provided handler port
func (w *Worker) Start() {
	log.Printf("starting http status server on port %d", w.HttpPort())

	http.HandleFunc("/", w.HandleRequest)
	go func() {
		err := http.ListenAndServe(":"+strconv.Itoa(w.HttpPort()), nil)
		log.Fatal("http health service failed", err)
	}()

}

// NewWorker will create a new worker bootstrap application.
// *tskType: defines the worker type; the type of tasks the worker is expecting. Also acts as a name for identification (required)
// *mkr: MakeWorker function that the launcher will call to create a new worker.
// *options: a struct pointer to additional specific application config options. Note that
//          the bootstrapped Worker already provides bus and launcher config options and the user
//          can request to add postgres and mysql config options.
func NewWorkerApp(tskType string, newWkr task.NewWorker, options Validator) *Worker {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	if options == nil {
		options = &NilValidator{}
	}

	return &Worker{
		tskType:    tskType,
		newWkr:     newWkr,
		wkrOpt:     newWkrOptions(tskType),
		appOpt:     options,
		lgr:        log.New(os.Stderr, "", log.LstdFlags),
		statusPort: &statsOptions{HttpPort: 11000},
	}
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
func (w *Worker) Initialize() {
	w.setHelpOutput() // add description to help

	// flags
	w.handleFlags()

	// validate Worker options
	err := w.appOpt.Validate()
	if err != nil {
		w.logFatal(err)
	}

	// launcher
	w.l, err = task.NewLauncher(w.newWkr, w.wkrOpt.LauncherOpt, w.wkrOpt.BusOpt)
	if err != nil {
		w.logFatal(err)
	}
}

func (a *Worker) setHelpOutput() {
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

func (a *Worker) logFatal(err error) {
	a.lgr.SetFlags(0)
	if a.TaskType() != "" {
		a.lgr.SetPrefix(a.TaskType() + ": ")
	} else {
		a.lgr.SetPrefix("")
	}
	a.lgr.Fatalln(err.Error())
}

func (a *Worker) handleFlags() {
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

func (a *Worker) showVersion() {
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

func (a *Worker) genConfig() {
	var appOptB, wkrOptB, fileOptB, pgOptB, mysqlOptB, statsOptB []byte
	var err error

	// Worker options
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

	if a.statusPort != nil {
		statsOptB, _ = ptoml.Marshal(*a.statusPort)
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
	fmt.Print(string(statsOptB))
	fmt.Print(string(appOptB))
	fmt.Print(string(wkrOptB))
	fmt.Print(string(fileOptB))
	fmt.Print(string(pgOptB))
	fmt.Print(string(mysqlOptB))

	os.Exit(0)
}

func (a *Worker) loadOptions(cpth string) error {
	// status options
	if _, err := btoml.DecodeFile(cpth, a.statusPort); err != nil {
		return err
	}

	// Worker options
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
func (w *Worker) Run() {
	// Start the http health status service
	w.Start()

	// do tasks
	done, cncl := w.l.DoTasks()
	w.Log("listening for %s tasks on '%s'", w.wkrOpt.BusOpt.Bus, w.wkrOpt.BusOpt.InTopic)

	select {
	case <-sigChan:
		cncl()
		<-done.Done()
	case <-done.Done():
	}

	os.Exit(0)
}

// HttpPort gets the application http port for requesting
// a heath check on the application itself. If the port is not provided
// the next available system port will be used. ie ':0'
func (w *Worker) HttpPort() int {
	return w.statusPort.HttpPort
}

// Version sets the application version. The version
// is what is shown if the '-version' flag is specified
// when running the Worker.
func (w *Worker) Version(version string) *Worker {
	w.version = version
	return w
}

// Description allows the user to set a description of the
// worker that will be shown with the help screen.
//
// The description should also include information about
// what the worker expects from the NewWorker 'info' string.
func (a *Worker) Description(description string) *Worker {
	a.description = description
	return a
}

// FileOpts provides file options such as aws connection info.
func (a *Worker) FileOpts() *Worker {
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
// connection options need to be made available with the Worker
// initialization. Note that the DBOptions struct is available
// to use in this way.
func (a *Worker) MySQLOpts() *Worker {
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
// connection options need to be made available with the Worker
// initialization. Note that the DBOptions struct is available
// to use in this way.
func (a *Worker) PostgresOpts() *Worker {
	if a.pgOpts == nil {
		a.pgOpts = &pgOptions{}
	}
	return a
}

func (a *Worker) GetFileOpts() *file.Options {
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
func (a *Worker) SetLogger(lgr *log.Logger) *Worker {
	if lgr != nil {
		a.lgr = lgr
	}

	return a
}

// TaskType returns the TaskType initialized with
// the Worker.
func (a *Worker) TaskType() string {
	return a.tskType
}

// MySQLDB returns the MySQL sql.DB application connection.
// Will be nil if called before Start() or MySQLOpts() was
// not called.
func (a *Worker) MySQLDB() *sql.DB {
	return a.mysql
}

// PostgresDB returns the Postgres sql.DB application connection.
// Will be nil if called before Start() or PostgresOpts() was
// not called.
func (a *Worker) PostgresDB() *sql.DB {
	return a.postgres
}

// Logger returns a reference to the application logger.
func (a *Worker) Logger() *log.Logger {
	return a.lgr
}

// NewConsumer is a convenience method that will use
// the bus config information to create a new consumer
// instance. Can optionally provide a topic and channel
// on which to consume. All other bus options are the same.
func (w *Worker) NewConsumer(topic, channel string) bus.Consumer {
	var err error
	busOpt := bus.NewOptions(w.wkrOpt.BusOpt.Bus)
	busOpt.InBus = w.wkrOpt.BusOpt.InBus
	busOpt.InTopic = w.wkrOpt.BusOpt.InTopic
	busOpt.InChannel = w.wkrOpt.BusOpt.InChannel
	busOpt.LookupdHosts = w.wkrOpt.BusOpt.LookupdHosts
	busOpt.NSQdHosts = w.wkrOpt.BusOpt.NSQdHosts

	if topic != "" {
		busOpt.InTopic = topic
	}

	if channel != "" {
		busOpt.InChannel = channel
	}

	w.c, err = bus.NewConsumer(busOpt)
	if err != nil {
		log.Fatal(err)
	}

	return w.c
}

// NewProducer will use the bus config information
// to create a new producer instance.
func (a *Worker) NewProducer() bus.Producer {
	var err error
	busOpt := bus.NewOptions(a.wkrOpt.BusOpt.Bus)
	busOpt.OutBus = a.wkrOpt.BusOpt.OutBus
	busOpt.LookupdHosts = a.wkrOpt.BusOpt.LookupdHosts
	busOpt.NSQdHosts = a.wkrOpt.BusOpt.NSQdHosts

	a.p, err = bus.NewProducer(a.wkrOpt.BusOpt)
	if err != nil {
		log.Fatal(err)
	}

	return a.p
}

// Log is a wrapper around the application logger Printf method.
func (a *Worker) Log(format string, v ...interface{}) {
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

// general options for http-status health checks
type statsOptions struct {
	HttpPort int `toml:"status_port" comment:"http service port for request health status"`
}

// appOptions provides general options available to
// all workers.
type wkrOptions struct {
	BusOpt      *bus.Options          `toml:"bus"`
	LauncherOpt *task.LauncherOptions `toml:"launcher"`
}

// fileOptions are only included at the request of the user.
// If added they are made available with the application file
// options object which can be accessed from the Worker object.
type fileOptions struct {
	FileOpt file.Options `toml:"file"`
}

// mysqlOptions are only added at the request of the user.
// If they are added then the bootstrap Worker will automatically
// attempt to connect to mysql.
type mysqlOptions struct {
	MySQL DBOptions `toml:"mysql"`
}

// postgresOptions are only added at the request of the user.
// If they are added then the bootstrap Worker will automatically
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
