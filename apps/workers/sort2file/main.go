package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

var (
	defaultFileTopic = "files"
	defaultTaskType  = "sort2file"
	fileBufPrefix    = "sort2file_"            // tmp file prefix
	sigChan          = make(chan os.Signal, 1) // app signal handling
	appOpt           *options                  // app options
	producer         bus.Producer              // special producer instance
)

func main() {
	flag.Parse()
	// set appOpt
	if err := loadAppOptions(); err != nil {
		log.Fatalln(err)
	}

	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// producer
	busOpt := cloneBusOpts(*appOpt.Options)
	if appOpt.FileTopic == "" || appOpt.FileTopic == "-" {
		busOpt.Bus = "nop" // disable producing
	}
	if producer, err = bus.NewProducer(&busOpt); err != nil {
		return err
	}

	// launcher
	l, err := task.NewLauncher(MakeWorker, appOpt.LauncherOptions, appOpt.Options)
	if err != nil {
		return err
	}
	done, cncl := l.DoTasks()

	select {
	case <-sigChan:
		cncl() // cancel launcher
		<-done.Done()
	case <-done.Done():
	}

	return err
}

var (
	confPth = flag.String("config", "config.toml", "toml config file path; over-written by flag values")
)

func newOptions() *options {
	return &options{
		LauncherOptions: task.NewLauncherOptions(),
		Options:         task.NewBusOptions(""),
	}
}

func cloneBusOpts(opt bus.Options) bus.Options { return opt }

type options struct {
	*task.LauncherOptions // launcher options
	*bus.Options          // bus options

	TaskType      string `toml:"task_type"`
	FileTopic     string `toml:"file_topic"`      // topic to publish information about written files
	FileBufferDir string `toml:"file_buffer_dir"` // if using a file buffer, use this base directory
	AWSAccessKey  string `toml:"aws_access_key"`  // required for s3 usage
	AWSSecretKey  string `toml:"aws_secret_key"`  // required for s3 usage
}

// nsqdHostsString will set Options.NSQdHosts from a
// comma-separated string of hosts.
func (opt *options) nsqdHostsString(hosts string) {
	opt.NSQdHosts = strings.Split(hosts, ",")
}

// loadAppOptions loads the applications
// options and sets those options to the
// global appOpt variable.
func loadAppOptions() error {
	opt := newOptions()
	opt.TaskType = defaultTaskType
	opt.FileTopic = defaultFileTopic

	// parse toml first - override with flag values
	_, err := toml.DecodeFile(*confPth, &opt)
	if err != nil {
		return err
	}

	if opt.Topic == "" {
		opt.Topic = opt.TaskType
	}

	if opt.Channel == "" {
		opt.Channel = opt.TaskType
	}

	appOpt = opt
	return nil
}
