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

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/db"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/bus/info"
	btoml "gopkg.in/BurntSushi/toml.v0"
	ptoml "gopkg.in/pelletier/go-toml.v1"
)

type Worker struct {
	tskType     string         // application task type
	version     string         // application version
	description string         // info help string that show expected info format
	newWkr      task.NewWorker // application MakeWorker function

	launcher *task.Launcher
	consumer bus.Consumer
	producer bus.Producer

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
	AppName       string             `json:"app_name"`
	LauncherStats task.LauncherStats `json:"launcher,omitempty"`
	ProducerStats *info.Producer     `json:"producer,omitempty"`
	ConsumerStats *info.Consumer     `json:"consumer,omitempty"`
}

// InfoStats for the Worker app
func (w *Worker) InfoStats() Info {
	w.Info.AppName = w.tskType

	if w.consumer != nil {
		cs := w.consumer.Info()
		w.ConsumerStats = &cs
	}

	if w.launcher != nil {
		w.LauncherStats = w.launcher.Stats()
	}

	if w.producer != nil {
		ps := w.producer.Info()
		w.ProducerStats = &ps
	}

	return w.Info
}

// HandleRequest is a simple http handler function that takes the compiled status functions
// that are called and the results marshaled to return as the body of the response
func (w *Worker) HandleRequest(wr http.ResponseWriter, r *http.Request) {
	wr.Header().Add("Content-Type", "application/json")
	b, _ := json.MarshalIndent(w.InfoStats(), "", "  ")

	wr.Write(b)
}

// Start will run the http server on the provided handler port
func (w *Worker) start() {
	if w.HttpPort() == 0 {
		log.Printf("http status server has been disabled")
		return
	}
	log.Printf("starting http status server on port %d", w.HttpPort())

	http.HandleFunc("/", w.HandleRequest)
	go func() {
		err := http.ListenAndServe(":"+strconv.Itoa(w.HttpPort()), nil)
		log.Fatal("http health service failed", err)
	}()
}

// NewWorkerApp will create a new worker bootstrap application.
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
		statusPort: &statsOptions{HttpPort: 0},
	}
}

// Initialize is non-blocking and will perform application startup
// tasks such as:
// *Parsing and handling flags
// *Parsing and validating the config file
// *Setting config defaults
//
// Note that Initialize will handle application closure if there
// was an error during startup or a flag option was provided
// that asked the application to show the version, for example.
// So, if Initialize is able to finish by returning, the user knows
// it is safe to move on.
func (w *Worker) Initialize() *Worker {
	w.setHelpOutput() // add description to help

	// flags
	w.handleFlags()

	// validate Worker options
	err := w.appOpt.Validate()
	if err != nil {
		w.logFatal(err)
	}

	// launcher
	w.launcher, err = task.NewLauncher(w.newWkr, w.wkrOpt.LauncherOpt, w.wkrOpt.BusOpt)
	if err != nil {
		w.logFatal(err)
	}
	return w
}

func (w *Worker) setHelpOutput() {
	// custom help screen
	flag.Usage = func() {
		if w.TaskType() != "" {
			fmt.Fprintln(os.Stderr, w.TaskType()+" worker")
			fmt.Fprintln(os.Stderr, "")
		}
		if w.description != "" {
			fmt.Fprintln(os.Stderr, w.description)
			fmt.Fprintln(os.Stderr, "")
		}
		fmt.Fprintln(os.Stderr, "Flag options:")
		flag.PrintDefaults()
	}
}

func (w *Worker) logFatal(err error) {
	w.lgr.SetFlags(0)
	if w.TaskType() != "" {
		w.lgr.SetPrefix(w.TaskType() + ": ")
	} else {
		w.lgr.SetPrefix("")
	}
	w.lgr.Fatalln(err.Error())
}

func (w *Worker) handleFlags() {
	if !flag.Parsed() {
		flag.Parse()
	}

	// version
	if *showVersion || *ver {
		w.showVersion()
	}

	// gen config (sent to stdout)
	if *genConfig || *g {
		w.genConfig()
	}

	var path string
	// configPth required
	if *configPth == "" && *c == "" {
		w.logFatal(errors.New("-config (-c) config file path required"))
	} else if *configPth != "" {
		path = *configPth
	} else {
		path = *c
	}

	// options

	err := w.loadOptions(path)
	if err != nil {
		w.logFatal(err)
	}
}

func (w *Worker) showVersion() {
	prefix := ""
	if w.TaskType() != "" {
		prefix = w.TaskType() + " "
	}
	if w.version == "" {
		fmt.Println(prefix + "version not specified")
	} else {
		fmt.Println(prefix + w.version)
	}
	os.Exit(0)
}

func (w *Worker) genConfig() {
	var appOptB, wkrOptB, fileOptB, pgOptB, mysqlOptB, statsOptB []byte
	var err error

	// Worker options
	appOptB, err = ptoml.Marshal(reflect.Indirect(reflect.ValueOf(w.appOpt)).Interface())
	if err != nil {
		w.lgr.SetFlags(0)
		if w.TaskType() != "" {
			w.lgr.SetPrefix(w.TaskType() + ": ")
		} else {
			w.lgr.SetPrefix("")
		}
		w.lgr.Fatalln(err.Error())
	}

	// worker options
	wkrOptB, err = ptoml.Marshal(*w.wkrOpt)
	if err != nil {
		w.lgr.SetFlags(0)
		if w.TaskType() != "" {
			w.lgr.SetPrefix(w.TaskType() + ": ")
		} else {
			w.lgr.SetPrefix("")
		}
		w.lgr.Fatalln(err.Error())
	}

	// file options
	if w.fileOpts != nil {
		fileOptB, err = ptoml.Marshal(*w.fileOpts)
	}
	if err != nil {
		w.lgr.SetFlags(0)
		if w.TaskType() != "" {
			w.lgr.SetPrefix(w.TaskType() + ": ")
		} else {
			w.lgr.SetPrefix("")
		}
		w.lgr.Fatalln(err.Error())
	}

	// postgres options
	if w.pgOpts != nil {
		pgOptB, err = ptoml.Marshal(*w.pgOpts)
	}
	if err != nil {
		w.lgr.SetFlags(0)
		if w.TaskType() != "" {
			w.lgr.SetPrefix(w.TaskType() + ": ")
		} else {
			w.lgr.SetPrefix("")
		}
		w.lgr.Fatalln(err.Error())
	}

	// mysql options
	if w.mysqlOpts != nil {
		mysqlOptB, err = ptoml.Marshal(*w.mysqlOpts)
	}

	if w.statusPort != nil {
		statsOptB, _ = ptoml.Marshal(*w.statusPort)
	}

	// err
	if err != nil {
		w.lgr.SetFlags(0)
		if w.TaskType() != "" {
			w.lgr.SetPrefix(w.TaskType() + ": ")
		} else {
			w.lgr.SetPrefix("")
		}
		w.lgr.Fatalln(err.Error())
	}

	fmt.Printf("# '%v' worker options\n", w.TaskType())
	fmt.Print(string(appOptB))
	fmt.Print(string(statsOptB))
	fmt.Print(string(wkrOptB))
	fmt.Print(string(fileOptB))
	fmt.Print(string(pgOptB))
	fmt.Print(string(mysqlOptB))

	os.Exit(0)
}

func (w *Worker) loadOptions(cpth string) error {
	// status options
	if _, err := btoml.DecodeFile(cpth, w.statusPort); err != nil {
		return err
	}

	// Worker options
	if _, err := btoml.DecodeFile(cpth, w.appOpt); err != nil {
		return err
	}

	// worker options
	if _, err := btoml.DecodeFile(cpth, w.wkrOpt); err != nil {
		return err
	}

	// file options
	if w.fileOpts != nil {
		_, err := btoml.DecodeFile(cpth, w.fileOpts)
		if err != nil {
			return err
		}
	}

	// postgres options (if requested)
	if w.pgOpts != nil {
		_, err := btoml.DecodeFile(cpth, w.pgOpts)
		if err != nil {
			return err
		}

		// connect
		pg := w.pgOpts.Postgres
		w.postgres, err = db.Postgres(pg.Username, pg.Password, pg.Host, pg.DBName)
		if err != nil {
			return err
		}
	}

	// mysql options (if requested)
	if w.mysqlOpts != nil {
		_, err := btoml.DecodeFile(cpth, w.mysqlOpts)
		if err != nil {
			return err
		}

		// connect
		mysql := w.mysqlOpts.MySQL
		w.mysql, err = db.MySQL(mysql.Username, mysql.Password, mysql.Host, mysql.DBName)
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
	w.start()

	// do tasks
	done, cncl := w.launcher.DoTasks()
	w.Log("listening for %s tasks on '%s'", w.wkrOpt.BusOpt.InBus, w.wkrOpt.BusOpt.InTopic)

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
// The port should alwasy be provided
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
func (w *Worker) Description(description string) *Worker {
	w.description = description
	return w
}

// FileOpts provides file options such as aws connection info.
func (w *Worker) FileOpts() *Worker {
	if w.fileOpts == nil {
		w.fileOpts = &fileOptions{}
		w.fileOpts.FileOpt.FileBufPrefix = w.TaskType()
	}
	return w
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
func (w *Worker) MySQLOpts() *Worker {
	if w.mysqlOpts == nil {
		w.mysqlOpts = &mysqlOptions{}
	}
	return w
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
func (w *Worker) PostgresOpts() *Worker {
	if w.pgOpts == nil {
		w.pgOpts = &pgOptions{}
	}
	return w
}

func (w *Worker) GetFileOpts() *file.Options {
	if w.fileOpts == nil {
		return nil
	}
	return &w.fileOpts.FileOpt
}

// SetLogger allows the user to override the default
// application logger (stdout) with a custom one.
//
// If the provided logger is nil the logger output is discarded.
//
// SetLogger should be called before initializing the application.
func (w *Worker) SetLogger(lgr *log.Logger) *Worker {
	if lgr != nil {
		w.lgr = lgr
	}

	return w
}

// TaskType returns the TaskType initialized with
// the Worker.
func (w *Worker) TaskType() string {
	return w.tskType
}

// MySQLDB returns the MySQL sql.DB application connection.
// Will be nil if called before Start() or MySQLOpts() was
// not called.
func (w *Worker) MySQLDB() *sql.DB {
	return w.mysql
}

// PostgresDB returns the Postgres sql.DB application connection.
// Will be nil if called before Start() or PostgresOpts() was
// not called.
func (w *Worker) PostgresDB() *sql.DB {
	return w.postgres
}

// Logger returns a reference to the application logger.
func (w *Worker) Logger() *log.Logger {
	return w.lgr
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

	w.consumer, err = bus.NewConsumer(busOpt)
	if err != nil {
		log.Fatal(err)
	}

	return w.consumer
}

// NewProducer will use the bus config information
// to create a new producer instance.
func (w *Worker) NewProducer() bus.Producer {
	var err error
	busOpt := bus.NewOptions(w.wkrOpt.BusOpt.Bus)
	busOpt.OutBus = w.wkrOpt.BusOpt.OutBus
	busOpt.LookupdHosts = w.wkrOpt.BusOpt.LookupdHosts
	busOpt.NSQdHosts = w.wkrOpt.BusOpt.NSQdHosts

	w.producer, err = bus.NewProducer(w.wkrOpt.BusOpt)
	if err != nil {
		log.Fatal(err)
	}

	return w.producer
}

// Log is a wrapper around the application logger Printf method.
func (w *Worker) Log(format string, v ...interface{}) {
	w.lgr.Printf(format, v...)
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
