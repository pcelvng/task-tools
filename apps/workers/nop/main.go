package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pcelvng/task"
)

var (
	sigChan = make(chan os.Signal, 1)

	// appOpt contains the application options
	appOpt options
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() error {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// appOpt
	loadAppOptions()
	if err := appOpt.Validate(); err != nil {
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
		cncl()
		<-done.Done()
	case <-done.Done():
	}

	if err := l.Err(); err != nil {
		return err
	}

	return nil
}
