package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

var (
	defaultFileTopic = "files"

	confPth = flag.String("config", "config.toml", "file path for toml config file")

	sigChan       = make(chan os.Signal, 1)
	fileBufPrefix = "deduper_" // tmp file prefix
	appOpt        *options
	producer      bus.Producer // special producer instance for file stats
)

func main() {
	if err := loadOptions(); err != nil {
		log.Fatal(err)
	}

	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	// signal handling - capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// producer
	busOpt := cloneBusOpts(*appOpt.Bus)
	if appOpt.FileTopic == "" || appOpt.FileTopic == "-" {
		busOpt.Bus = "nop" // disable producing
	}
	if producer, err = bus.NewProducer(&busOpt); err != nil {
		return err
	}

	// launcher
	l, err := task.NewLauncher(NewWorker, appOpt.Launcher, appOpt.Bus)
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
	return nil
}

func cloneBusOpts(opt bus.Options) bus.Options { return opt }

func newOptions() *options {
	return &options{
		Bus:      bus.NewOptions(""),
		Launcher: task.NewLauncherOptions(),
	}
}

type options struct {
	Bus      *bus.Options
	Launcher *task.LauncherOptions

	FileTopic     string // topic with file stats (default=files but can be turned off by setting it to "-")
	FileBufferDir string `toml:"file_buffer_dir"` // if using a file buffer, use this base directory
	AWSAccessKey  string `toml:"aws_access_key"`  // required for s3 usage
	AWSSecretKey  string `toml:"aws_secret_key"`  // required for s3 usage
}

func loadOptions() error {
	flag.Parse()

	appOpt = newOptions()
	appOpt.FileTopic = defaultFileTopic

	_, err := toml.DecodeFile(*confPth, appOpt)

	return err
}
