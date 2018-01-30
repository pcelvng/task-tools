package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

var (
	fileBufPrefix = "sortbyhour_"           // tmp file prefix
	sigChan       = make(chan os.Signal, 1) // app signal handling
	appOpt        options                   // app options
	producer      bus.Producer              // special producer instance
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	// signal handling - be ready to capture signal early.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// set appOpt
	if err = loadAppOptions(); err != nil {
		return err
	}

	// producer
	busOpt := cloneBusOpts(*appOpt.Bus)
	if appOpt.FileTopic == "" || appOpt.FileTopic == "-" {
		busOpt.Bus = "nop" // disable producing
	}
	if producer, err = bus.NewProducer(&busOpt); err != nil {
		return err
	}

	// launcher
	l, err := task.NewLauncher(MakeWorker, appOpt.Launcher, appOpt.Bus)
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
	confPth            = flag.String("config", "", "toml config file path; over-written by flag values")
	tskType            = flag.String("type", "sortbyhour", "task type; also is the default topic value")
	tskBus             = flag.String("bus", "stdio", "'stdio', 'file', 'nsq'")
	inBus              = flag.String("in-bus", "", "one of 'stdin', 'file', 'nsq'; useful if you want the in and out bus to be different types")
	outBus             = flag.String("out-bus", "", "one of 'stdout', 'file', 'nsq'; useful if you want the in and out bus to be different types")
	inFile             = flag.String("in-file", "./in.tsks.json", "file bus path and name when 'file' task-bus specified")
	outFile            = flag.String("out-file", "./out.tsks.json", "file bus path and name when 'file' task-bus specified")
	nsqdHosts          = flag.String("nsqd-hosts", "localhost:4150", "comma-separated list of nsqd hosts with tcp port")
	lookupdHosts       = flag.String("lookupd-hosts", "localhost:4161", "comma-separated list of lookupd hosts with http port")
	topic              = flag.String("topic", "", "topic for consuming tasks; default is task type value")
	channel            = flag.String("channel", "", "channel on which to consume tasks; default is task type value")
	doneTopic          = flag.String("done-topic", "done", "topic to publish done tasks")
	fileTopic          = flag.String("file-topic", "files", "topic to publish file stats of written files; use '-' to disable")
	awsAccessKey       = flag.String("aws-access-key", "", "required for s3 usage")
	awsSecretKey       = flag.String("aws-secret-key", "", "required for s3 usage")
	maxInProgress      = flag.Uint("max-in-progress", 1, "maximum number of workers running at one time; a value of 0 is set to 1")
	workerTimeout      = flag.Duration("worker-timeout", time.Second*10, "time duration to wait for a worker to finish under forced application shutdown")
	lifetimeMaxWorkers = flag.Uint("lifetime-max-workers", 0, "maximum number of tasks that will be completed before the application will shutdown; a negative value sets no limit")
)

func newOptions() options {
	return options{
		Launcher: task.NewLauncherOptions(),
		Bus:      task.NewBusOptions(""),
	}
}

func cloneBusOpts(opt bus.Options) bus.Options { return opt }

type options struct {
	Launcher *task.LauncherOptions `toml:"launcher"` // launcher options
	Bus      *bus.Options          `toml:"bus"`      // bus options

	FileTopic     string `toml:"file_topic"`      // topic to publish information about written files
	FileBufferDir string `toml:"file_buffer_dir"` // if using a file buffer, use this base directory
	AWSAccessKey  string `toml:"aws_access_key"`  // required for s3 usage
	AWSSecretKey  string `toml:"aws_secret_key"`  // required for s3 usage
}

// nsqdHostsString will set Options.NSQdHosts from a
// comma-separated string of hosts.
func (opt *options) nsqdHostsString(hosts string) {
	opt.Bus.NSQdHosts = strings.Split(hosts, ",")
}

// loadAppOptions loads the applications
// options and sets those options to the
// global appOpt variable.
func loadAppOptions() error {
	flag.Parse()
	opt := newOptions()

	// parse toml first - override with flag values
	if *confPth != "" {
		_, err := toml.DecodeFile(*confPth, &opt)
		if err != nil {
			return err
		}
	}

	// load config
	if *tskBus != "" {
		opt.Bus.Bus = *tskBus
	}
	if *inBus != "" {
		opt.Bus.InBus = *inBus
	}
	if *outBus != "" {
		opt.Bus.OutBus = *outBus
	}
	if *inFile != "" {
		opt.Bus.InFile = *inFile
	}
	if *outFile != "" {
		opt.Bus.OutFile = *outFile
	}
	if opt.Bus.Topic == "" {
		opt.Bus.Topic = *tskType // default consumer topic
	}
	if *topic != "" {
		opt.Bus.Topic = *topic
	}
	if opt.Bus.Channel == "" {
		opt.Bus.Channel = *tskType // default consumer channel
	}
	if *channel != "" {
		opt.Bus.Channel = *channel
	}
	if opt.Launcher.TaskType == "" {
		opt.Launcher.TaskType = *tskType
	}
	if *doneTopic != "done" {
		opt.Launcher.DoneTopic = *doneTopic
	}
	if *maxInProgress != 0 {
		opt.Launcher.MaxInProgress = *maxInProgress
	}
	if *workerTimeout != time.Duration(0) {
		opt.Launcher.WorkerTimeout = *workerTimeout
	}
	if *lifetimeMaxWorkers != 0 {
		opt.Launcher.LifetimeMaxWorkers = *lifetimeMaxWorkers
	}
	opt.nsqdHostsString(*nsqdHosts)

	appOpt = opt
	return nil
}
