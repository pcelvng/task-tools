package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/davecgh/go-spew/spew"
	"github.com/hydronica/go-config"
	"github.com/hydronica/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

type NewRunner func(*TaskMaster) Runner

type Runner interface {
	Run(ctx context.Context) error
	Info() interface{}
}

// NewTaskMaster will create a new taskmaster bootstrap application.
// *appName: defines the taskmaster name; acts as a name for identification and easy-of-use (required)
// *mkr: MakeWorker function that the launcher will call to create a new worker.
// *options: a struct pointer to additional specific application options options. Note that
//
//	the bootstrapped WorkerApp already provides bus and launcher options options and the user
//	can request to add postgres and mysql options options.
func NewTaskMaster(appName string, initFn NewRunner, options Validator) *TaskMaster {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if options == nil {
		options = &NilValidator{}
	}

	tm := &TaskMaster{
		Utility: Utility{
			options: options,
			name:    appName,
		},
		newRunner: initFn,
	}
	tm.infoFn = tm.HandleRequest
	return tm
}

func (tm *TaskMaster) AppOpt() interface{} {
	return tm.options
}

type TaskMaster struct {
	StatusPort int `toml:"status_port" comment:"http service port for request health status"`
	Utility

	newRunner NewRunner
	runner    Runner
	
	// options
	BusOpt      *bus.Options          `toml:"bus"`
	LauncherOpt *task.LauncherOptions `toml:"launcher"`

	infoFn func(w http.ResponseWriter, r *http.Request)
}

// Initialize is non-blocking and will perform application startup
// tasks such as:
// *Parsing and handling flags
// *Parsing and validating the options file
// *Setting options defaults
//
// Note that start will handle application closure if there
// was an error during startup or a flag option was provided
// that asked the application to show the version, for example.
// So, if start is able to finish by returning, the user knows
// it is safe to move on.
func (tm *TaskMaster) Initialize() *TaskMaster {
	var genConf bool
	var showConf bool
	flag.BoolVar(&genConf, "g", false, "generate options file")
	flag.BoolVar(&showConf, "show", false, "show current options values")
	config.New(tm).
		Version(tm.version).Disable(config.OptGenConf | config.OptShow).
		Description(tm.description).
		LoadOrDie()

	if genConf {
		tm.genConfig()
		os.Exit(0)
	}
	if showConf {
		spew.Dump(tm.StatusPort)
		spew.Dump(tm.options)
		spew.Dump(tm.BusOpt)
		spew.Dump(tm.LauncherOpt)
		os.Exit(0)
	}

	tm.runner = tm.newRunner(tm)
	return tm
}

func (tm *TaskMaster) genConfig() {
	writer := os.Stdout
	writer.WriteString("status_port = 0\n\n")
	enc := toml.NewEncoder(writer)
	if err := enc.Encode(tm.options); err != nil {
		log.Fatal(err)
	}
	writer.Write([]byte("\n"))
	writer.WriteString(genBusOptions(tm.BusOpt))
	writer.Write([]byte("\n"))
	writer.WriteString(genLauncherOptions(tm.LauncherOpt))

	os.Exit(0)
}

// HandleRequest is a simple http handler function that takes the compiled status functions
// that are called and the results marshaled to return as the body of the response
func (tm *TaskMaster) HandleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	b, err := json.MarshalIndent(tm.runner.Info(), "", "  ")
	s := fmt.Sprintf(`{
  "app_name":"%s",
  "version":"%s",`, tm.name, tm.version)
	if b != nil && err == nil {
		// Replace the first { in the json string with the { + application name
		b = bytes.Replace(b, []byte(`{`), []byte(s), 1)
	}
	w.Write(b)
}

// SetHandler will overwrite the current HandleRequest function on root requests
func (tm *TaskMaster) SetHandler(fn func(http.ResponseWriter, *http.Request)) {
	tm.infoFn = fn
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

	log.Println("starting %v", tm.name)
	if err := tm.runner.Run(ctx); err != nil {
		log.Fatal(err)
	}

	os.Exit(0)
}

// Start will run the http server on the provided handler port
func (tm *TaskMaster) start() {
	if tm.StatusPort == 0 {
		log.Printf("http status server has been disabled")
		return
	}
	log.Printf("starting http status server on port %d", tm.StatusPort)

	http.HandleFunc("/", tm.infoFn)
	go func() {
		err := http.ListenAndServe(":"+strconv.Itoa(tm.StatusPort), nil)
		log.Fatal("http health service failed", err)
	}()
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

// NewConsumer is a convenience method that will use
// the bus options information to create a new consumer
// instance. Can optionally provide a topic and channel
// on which to consume. All other bus options are the same.
func (tm *TaskMaster) NewConsumer() bus.Consumer {
	var busOpt bus.Options
	if tm != nil {
		busOpt = *tm.BusOpt
	}
	return newConsumer(busOpt, busOpt.InTopic, busOpt.InChannel)
}

// NewProducer will use the bus options information
// to create a new producer instance.
func (tm *TaskMaster) NewProducer() bus.Producer {
	var busOpt bus.Options
	if tm != nil {
		busOpt = *tm.BusOpt
	}
	return newProducer(busOpt)
}
