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
	"strings"
	"syscall"

	"github.com/davecgh/go-spew/spew"
	"github.com/hydronica/go-config"
	"github.com/hydronica/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

type NewRunner func(*Starter) Runner

type Runner interface {
	Run(ctx context.Context) error
	Info() interface{}
}

// NewTaskMaster will create a new taskmaster bootstrap application.
// *appName: defines the taskmaster name; acts as a name for identification and easy-of-use (required)
// *mkr: MakeWorker function that the launcher will call to create a new worker.
// *options: a struct pointer to additional specific application options.
// Note that the bootstrapped WorkerApp already provides bus and launcher options
func NewTaskMaster(appName string, initFn NewRunner, options Validator) *Starter {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if options == nil {
		options = &NilValidator{}
	}

	tm := &Starter{
		Utility: Utility{
			Validator: options,
			name:      appName,
		},
		bType:     "master",
		newRunner: initFn,
	}
	tm.infoFn = tm.HandleRequest
	return tm
}

// NewWorkerApp will create a new worker bootstrap application.
// *tskType: defines the worker type; the type of tasks the worker is expecting. Also acts as a name for identification (required)
// *mkr: MakeWorker function that the launcher will call to create a new worker.
// *options: a struct pointer to additional specific application options.
// Note that the bootstrapped Worker already provides bus and launcher options
func NewWorkerApp(tskType string, newWkr task.NewWorker, options Validator) *Starter {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if options == nil {
		options = &NilValidator{}
	}

	tm := &Starter{
		Utility: Utility{
			name:      tskType,
			Validator: options,
		},
		bType:  "worker",
		newWkr: newWkr,

		BusOpt: bus.Options{
			Bus:       "stdio",
			InTopic:   tskType,
			InChannel: tskType,
		},
		LauncherOpt: task.NewLauncherOptions(tskType),
	}
	tm.SetHandler(
		func(wr http.ResponseWriter, r *http.Request) {
			wr.Header().Add("Content-Type", "application/json")
			b, _ := json.MarshalIndent(tm.infoStats(), "", "  ")

			wr.Write(b)
		})
	return tm
}

func getFlagConfigPath() string {
	for i, arg := range os.Args {
		// Check for -c or -config flags
		if arg == "-c" || arg == "-config" {
			// Make sure there's a next argument
			if i+1 < len(os.Args) {
				return os.Args[i+1]
			}
		}
		// Check for -c= or -config= style flags
		if strings.HasPrefix(arg, "-c=") {
			return arg[3:]
		}
		if strings.HasPrefix(arg, "-config=") {
			return arg[8:]
		}
	}
	return ""
}

func (tm *Starter) AppOpt() any {
	return tm.Validator
}

type Starter struct {
	Utility    `toml:"-"`
	StatusPort int `toml:"status_port" comment:"http service port for request health status"`

	// options
	BusOpt      bus.Options           `toml:"bus"`
	LauncherOpt *task.LauncherOptions `toml:"launcher"`

	// Type of bootstrap: worker or Starter
	bType  string `toml:"-" flag:"-" env:"-"`
	infoFn func(w http.ResponseWriter, r *http.Request)

	// Starter vars
	newRunner NewRunner
	runner    Runner

	// Worker vars
	newWkr   task.NewWorker // application MakeWorker function
	launcher *task.Launcher
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
func (tm *Starter) Initialize() *Starter {
	var genConf bool
	var showConf bool
	flag.BoolVar(&genConf, "g", false, "generate options file")
	flag.BoolVar(&showConf, "show", false, "show current options values")
	config.New(tm).
		Version(tm.version).Disable(config.OptGenConf | config.OptShow | config.OptFlag | config.OptEnv).
		Description(tm.description).
		LoadOrDie()

	// Load the app options like a flat file
	p := getFlagConfigPath()
	if p != "" {
		if err := config.LoadFile(p, tm.Validator); err != nil {
			log.Fatal(err)
		}
	}

	if genConf {
		tm.genConfig()
		os.Exit(0)
	}
	if showConf {
		spew.Dump(tm.StatusPort)
		spew.Dump(tm.Validator)
		spew.Dump(tm.BusOpt)
		spew.Dump(tm.LauncherOpt)
		os.Exit(0)
	}
	switch tm.bType {
	case "worker":
		var err error
		tm.launcher, err = task.NewLauncher(tm.newWkr, tm.LauncherOpt, &tm.BusOpt)
		if err != nil {
			log.Fatal(err)
		}
	case "master":
		tm.runner = tm.newRunner(tm)
	}
	return tm
}

func (tm *Starter) genConfig() {
	writer := os.Stdout
	writer.WriteString("status_port = 0\n\n")
	enc := toml.NewEncoder(writer)
	if err := enc.Encode(tm.Validator); err != nil {
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
func (tm *Starter) HandleRequest(w http.ResponseWriter, r *http.Request) {
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
func (tm *Starter) SetHandler(fn func(http.ResponseWriter, *http.Request)) {
	tm.infoFn = fn
}

// Run until the application is complete and then exit.
func (tm *Starter) Run() {
	// Start the http health status service
	tm.start()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-sigChan:
			cancel()
		}
	}()

	log.Printf("starting %v", tm.name)
	switch tm.bType {
	case "worker":
		// do tasks
		log.Printf("listening for %s tasks on '%s'", tm.BusOpt.InBus, tm.BusOpt.InTopic)
		tm.launcher.Start(ctx)
	case "master":
		if err := tm.runner.Run(ctx); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown bootstrap type %q", tm.bType)
	}
	os.Exit(0)
}

// Start will run the http server on the provided handler port
func (tm *Starter) start() {
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

type Info struct {
	AppName       string             `json:"app_name"`
	Version       string             `json:"version"`
	LauncherStats task.LauncherStats `json:"launcher,omitempty"`
}

// infoStats for the Worker app
func (tm *Starter) infoStats() any {
	i := Info{
		AppName: tm.name,
		Version: tm.version,
	}

	if tm.launcher != nil {
		i.LauncherStats = tm.launcher.Stats()
	}

	return i
}

// Version sets the application version. The version
// is what is shown if the '-version' flag is specified
// when running the WorkerApp.
func (tm *Starter) Version(version string) *Starter {
	tm.version = version
	return tm
}

// Description allows the user to set a description of the
// worker that will be shown with the help screen.
//
// The description should also include information about
// what the worker expects from the NewWorker 'info' string.
func (tm *Starter) Description(description string) *Starter {
	tm.description = description
	return tm
}

// NewConsumer is a convenience method that will use
// the bus options information to create a new consumer
// instance. Can optionally provide a topic and channel
// on which to consume. All other bus options are the same.
func (tm *Starter) NewConsumer() bus.Consumer {
	return newConsumer(tm.BusOpt, tm.BusOpt.InTopic, tm.BusOpt.InChannel)
}

// NewProducer will use the bus options information
// to create a new producer instance.
func (tm *Starter) NewProducer() bus.Producer {
	return newProducer(tm.BusOpt)
}
