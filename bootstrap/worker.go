package bootstrap

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/davecgh/go-spew/spew"
	"github.com/hydronica/go-config"
	"github.com/hydronica/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/bus/info"
)

type Worker struct {
	Utility
	StatusPort int `toml:"status_port" comment:"http service port for request health status"`

	BusOpt      *bus.Options          `toml:"bus"`
	LauncherOpt *task.LauncherOptions `toml:"launcher"`

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
func (w *Worker) InfoStats() any {
	w.info.AppName = w.name
	w.info.Version = w.version

	if w.launcher != nil {
		w.info.LauncherStats = w.launcher.Stats()
	}

	return w.info
}

// NewWorkerApp will create a new worker bootstrap application.
// *tskType: defines the worker type; the type of tasks the worker is expecting. Also acts as a name for identification (required)
// *mkr: MakeWorker function that the launcher will call to create a new worker.
// *options: a struct pointer to additional specific application options options. Note that
//
//	the bootstrapped Worker already provides bus and launcher options options and the user
//	can request to add postgres and mysql options options.
func NewWorkerApp(tskType string, newWkr task.NewWorker, options Validator) *Worker {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if options == nil {
		options = &NilValidator{}
	}

	return &Worker{
		Utility: Utility{
			name:    tskType,
			options: options,
		},
		newWkr: newWkr,

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
	flag.BoolVar(&genConf, "g", false, "generate options file")
	flag.BoolVar(&showConf, "show", false, "show current options values")
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
	w.AddInfo(w.InfoStats, w.StatusPort)

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

func (w *Worker) NewProducer() bus.Producer {
	p, _ := bus.NewProducer(w.BusOpt)
	return p
}

func (w *Worker) Version(version string) *Worker {
	w.version = version
	return w
}

func (w *Worker) Description(description string) *Worker {
	w.description = description
	return w
}
