package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	tskType            = flag.String("type", "", "REQUIRED the task type; default topic")
	tskBus             = flag.String("bus", "stdio", "'stdio', 'file', 'nsq'")
	inBus              = flag.String("in-bus", "", "one of 'stdin', 'file', 'nsq'; useful if you want the in and out bus to be different types.")
	outBus             = flag.String("out-bus", "", "one of 'stdout', 'file', 'nsq'; useful if you want the in and out bus to be different types.")
	inFile             = flag.String("in-file", "./in.tsks.json", "file bus path and name when 'file' task-bus specified")
	outFile            = flag.String("out-file", "./out.tsks.json", "file bus path and name when 'file' task-bus specified")
	nsqdHosts          = flag.String("nsqd-hosts", "localhost:4150", "comma-separated list of nsqd hosts with tcp port")
	lookupdHosts       = flag.String("lookupd-hosts", "localhost:4161", "comma-separated list of lookupd hosts with http port")
	topic              = flag.String("topic", "", "override task type as topic")
	channel            = flag.String("channel", "", "override task type as channel")
	doneTopic          = flag.String("done-topic", "done", "topic to publish done tasks")
	fileTopic          = flag.String("file-topic", "files", "topic to publish file stats of written files; use '-' to disable")
	maxInProgress      = flag.Int("max-in-progress", 1, "maximum number of workers running at one time; workers cannot be less than 1.")
	workerTimeout      = flag.Duration("worker-timeout", time.Second*10, "time to wait for a worker to finish when being asked to shut down.")
	lifetimeMaxWorkers = flag.Int("lifetime-max-workers", 0, "maximum number of tasks that will be completed before the application will shut down. A value less than one sets no limit.")

	// fileBufPrefix is the prefix in front of tmp file names
	// if using a file buffer while writing.
	fileBufPrefix = "sortbyhour_"
	sigChan       = make(chan os.Signal, 1)
	appOpt        options
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// extract date
	//t, err := w.extractDate(ln)
	//if err != nil {
	//	return err
	//}
	return err
}

type options struct {
	FileBufferDir    string // if using a file buffer, use this base directory
	FileBufferPrefix string
}
