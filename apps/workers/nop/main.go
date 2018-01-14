package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pcelvng/task"
)

var sigChan = make(chan os.Signal, 1)

func main() {
	err := run()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
}

func run() error {
	// signal handling - capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// config
	config = LoadConfig()
	if err := config.Validate(); err != nil {
		return err
	}

	// launcher
	l, err := task.NewLauncher(MakeWorker, config.LauncherOpt, config.BusOpt)
	if err != nil {
		return err
	}
	done, cncl := l.DoTasks()

	select {
	case <-sigChan:
		cncl()
		<-done.Done()
	case <-done.Done():
	}

	if err := l.Err(); err != nil {
		return err
	}

	return nil
}
