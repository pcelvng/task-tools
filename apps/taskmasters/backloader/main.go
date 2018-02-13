package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
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

	// app config
	appConf, err := LoadConfig()
	if err != nil {
		return err
	}

	// backloader
	bl, err := NewBackloader(appConf)
	if err != nil {
		return err
	}

	doneChan := make(chan error)
	go func() {
		_, err := bl.Backload()
		doneChan <- err
	}()

	select {
	case blErr := <-doneChan:
		return blErr
	case <-sigChan:
		if err := bl.Stop(); err != nil {
			return err
		}
	}
	return nil
}


