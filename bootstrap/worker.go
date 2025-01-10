package bootstrap

import (
	"encoding/json"
	"flag"
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
	"github.com/pcelvng/task/bus/info"
)

type Worker struct {
	StatusPort int `toml:"status_port" comment:"http service port for request health status"`

	options Validator `config:"-"`

	BusOpt      *bus.Options          `toml:"bus"`
	LauncherOpt *task.LauncherOptions `toml:"launcher"`

	tskType     string // application task type
	version     string // application version
	description string // info help string that show expected info format

	newWkr   task.NewWorker // application MakeWorker function
	launcher *task.Launcher

	info Info // info stats on various worker types
}

type Info struct {
	AppName       string             `json:"app_name"`
	Version       string             `json:"version"`
	LauncherStats task.LauncherStats `json:"launcher,omitempty"`
	ProducerStats *info.Producer     `json:"producer,omitempty"`
	ConsumerStats *info.Consumer     `json:"consumer,omitempty"`
}

// InfoStats for the Worker app
func (w *Worker) InfoStats() Info {
	w.info.AppName = w.tskType
	w.info.Version = w.version

	if w.launcher != nil {
		w.info.LauncherStats = w.launcher.Stats()
	}

	return w.info
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
//
//	the bootstrapped Worker already provides bus and launcher config options and the user
//	can request to add postgres and mysql config options.
func NewWorkerApp(tskType string, newWkr task.NewWorker, options Validator) *Worker {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if options == nil {
		options = &NilValidator{}
	}

	return &Worker{
		tskType: tskType,
		newWkr:  newWkr,
		options: options,
		BusOpt: &bus.Options{
			Bus:       "stdio",
			InTopic:   tskType,
			InChannel: tskType,
		},
		LauncherOpt: task.NewLauncherOptions(tskType),
	}
}

func (w *Worker) Initialize() *Worker {
	var genConf bool
	var showConf bool
	flag.BoolVar(&genConf, "g", false, "generate config file")
	flag.BoolVar(&showConf, "show", false, "show current config values")
	config.New(w).
		Version(w.version).Disable(config.OptGenConf | config.OptShow).
		Description(w.description).
		LoadOrDie()

	if genConf {
		w.genConfig()
		os.Exit(0)
	}
	if showConf {
		spew.Dump(w.StatusPort)
		spew.Dump(w.options)
		spew.Dump(w.BusOpt)
		spew.Dump(w.LauncherOpt)
		os.Exit(0)
	}

	var err error
	// launcher
	w.launcher, err = task.NewLauncher(w.newWkr, w.LauncherOpt, w.BusOpt)
	if err != nil {
		log.Fatal(err)
	}

	return w
}

// Run will run until the application is complete
// and then exit.
func (w *Worker) Run() {
	// Start the http health status service
	w.start()

	// do tasks
	done, cncl := w.launcher.DoTasks()
	log.Printf("listening for %s tasks on '%s'", w.BusOpt.InBus, w.BusOpt.InTopic)

	select {
	case <-sigChan:
		cncl()
		<-done.Done()
	case <-done.Done():
	}

	os.Exit(0)
}

func (w *Worker) genConfig() {
	writer := os.Stdout
	writer.WriteString("status_port = 0\n\n")
	enc := toml.NewEncoder(writer)
	if err := enc.Encode(w.options); err != nil {
		log.Fatal(err)
	}
	writer.Write([]byte("\n"))
	writer.WriteString(genBusOptions(w.BusOpt))
	writer.Write([]byte("\n"))
	writer.WriteString(genLauncherOptions(w.LauncherOpt))

	os.Exit(0)
}

// HttpPort gets the application http port for requesting
// a heath check on the application itself. If the port is not provided
// The port should alwasy be provided
func (w *Worker) HttpPort() int {
	return w.StatusPort
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

// TaskType returns the TaskType initialized with
// the Worker.
func (w *Worker) TaskType() string {
	return w.tskType
}

func (w *Worker) NewProducer() bus.Producer {
	p, _ := bus.NewProducer(w.BusOpt)
	return p
}
