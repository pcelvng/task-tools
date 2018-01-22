package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/dedup"
	"github.com/pcelvng/task/bus"
)

var filePath = flag.String("config", "", "file path for toml config file")

type config struct {
	Bus      bus.Options
	Launcher task.LauncherOptions

	FileTopic    string
	WorkerConfig dedup.Config
}

func main() {
	flag.Parse()
	if err := Run(); err != nil {
		log.Fatal(err)
	}
}

func Run() error {
	// signal handling - capture signal early.
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	c := &config{}
	if *filePath != "" {
		if _, err := toml.DecodeFile(*filePath, c); err != nil {
			return err
		}
	}

	l, err := task.NewLauncher(c.WorkerConfig.NewWorker, &c.Launcher, &c.Bus)
	if err != nil {
		return err
	}
	_, cancelfn := l.DoTasks()

	select {
	case <-sigChan:
		fmt.Println("Stopping ...")
		cancelfn()
	}
	return nil
}
