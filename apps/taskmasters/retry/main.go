package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	config  = flag.String("config", "config.toml", "relative or absolute file path")
	sigChan = make(chan os.Signal, 1)
)

func main() {
	err := run()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
}

func run() error {
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	flag.Parse()
	conf, err := LoadConfig(*config)
	if err != nil {
		return err
	}

	// make retryer
	r, err := NewRetry(conf)
	if err != nil {
		return err
	}

	select {
	case <-sigChan:
		log.Println("closing...")

		err = r.Close()
		return err
	}
}
