package main

func main() {}

//import (
//	"flag"
//	"log"
//	"os"
//	"os/signal"
//	"strings"
//	"syscall"
//	"time"
//
//	"github.com/BurntSushi/toml"
//	"github.com/pcelvng/task"
//	"github.com/pcelvng/task/bus"
//)
//
//var (
//	fileBufPrefix = "sortbyhour_"           // tmp file prefix
//	sigChan       = make(chan os.Signal, 1) // app signal handling
//	appOpt        options                   // app options
//	producer      bus.Producer              // special producer instance
//)
//
//func main() {
//	if err := run(); err != nil {
//		log.Fatalln(err)
//	}
//}
//
//func run() (err error) {
//	// signal handling - be ready to capture signal early.
//	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
//
//	// set appOpt
//	if err = loadAppOptions(); err != nil {
//		return err
//	}
//
//	<-sigChan
//	return err
//}
//
//var config = flag.String("config", "config.toml", "relative or absolute file path")
//
//func newOptions() options {
//	return options{
//		Options: task.NewBusOptions(""),
//	}
//}
//
//func cloneBusOpts(opt bus.Options) bus.Options { return opt }
//
//type options struct {
//	*bus.Options `toml:"bus"` // bus options
//
//	Rules []Rule `toml:"rule"`
//}
//
//type Rule struct {
//	TaskType   string `toml:"type"`     // also default topic
//	SrcPattern string `toml:"template"` // source file path pattern to match
//	Topic      string `toml:"topic"`    // topic override
//
//	// checks for rules that checks on groups of files instead of responding
//	// immediately to an individual file.
//	CronCheck  string `toml:"cron_check"`  // cron parsable string representing when to check src pattern matching files
//	CountCheck int    `toml:"count_check"` //
//}
//
//// nsqdHostsString will set Options.NSQdHosts from a
//// comma-separated string of hosts.
//func (opt *options) nsqdHostsString(hosts string) {
//	opt.NSQdHosts = strings.Split(hosts, ",")
//}
//
//// loadAppOptions loads the applications
//// options and sets those options to the
//// global appOpt variable.
//func loadAppOptions() error {
//	flag.Parse()
//	opt := newOptions()
//
//	// parse toml first - override with flag values
//	if *confPth != "" {
//		_, err := toml.DecodeFile(*confPth, &opt)
//		if err != nil {
//			return err
//		}
//	}
//
//	// load config
//	if *tskBus != "" {
//		opt.Bus.Bus = *tskBus
//	}
//	if *inBus != "" {
//		opt.Bus.InBus = *inBus
//	}
//	if *outBus != "" {
//		opt.Bus.OutBus = *outBus
//	}
//	if *inFile != "" {
//		opt.Bus.InFile = *inFile
//	}
//	if *outFile != "" {
//		opt.Bus.OutFile = *outFile
//	}
//	if opt.Bus.Topic == "" {
//		opt.Bus.Topic = *tskType // default consumer topic
//	}
//	if *topic != "" {
//		opt.Bus.Topic = *topic
//	}
//	if opt.Bus.Channel == "" {
//		opt.Bus.Channel = *tskType // default consumer channel
//	}
//	if *channel != "" {
//		opt.Bus.Channel = *channel
//	}
//	if opt.Launcher.TaskType == "" {
//		opt.Launcher.TaskType = *tskType
//	}
//	if *doneTopic != "done" {
//		opt.Launcher.DoneTopic = *doneTopic
//	}
//	if *maxInProgress != 0 {
//		opt.Launcher.MaxInProgress = *maxInProgress
//	}
//	if *workerTimeout != time.Duration(0) {
//		opt.Launcher.WorkerTimeout = *workerTimeout
//	}
//	if *lifetimeMaxWorkers != 0 {
//		opt.Launcher.LifetimeMaxWorkers = *lifetimeMaxWorkers
//	}
//	opt.nsqdHostsString(*nsqdHosts)
//
//	appOpt = opt
//	return nil
//}
