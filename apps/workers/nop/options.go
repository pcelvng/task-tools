package main

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
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
	doneTopic          = flag.String("done-topic", "done", "topic to return the task after completion")
	failRate           = flag.Int("fail-rate", 0, "choose 0-100; the rate at which tasks will be marked with an error; does not support fractions of a percentage.")
	dur                = flag.String("duration", "1s", "'1s' = 1 second, '1m' = 1 minute, '1h' = 1 hour")
	durVariance        = flag.String("variance", "", "+ evenly distributed variation when a task completes; 1s = 1 second, 1m = 1 minute, 1h = 1 hour")
	maxInProgress      = flag.Int("max-in-progress", 1, "maximum number of workers running at one time; workers cannot be less than 1.")
	workerTimeout      = flag.Duration("worker-timeout", time.Second*10, "time to wait for a worker to finish when being asked to shut down.")
	lifetimeMaxWorkers = flag.Int("lifetime-max-workers", 0, "maximum number of tasks that will be completed before the application will shut down. A value less than one sets no limit.")
)

func newOptions() options {
	return options{
		LauncherOptions: task.NewLauncherOptions(),
		Options:         task.NewBusOptions(""),
	}
}

type options struct {
	*task.LauncherOptions               // launcher options
	*bus.Options                        // task message bus options
	TaskType              string        // will be used as the default topic and channel
	Topic                 string        // topic override (uses 'TaskType' if not provided)
	Channel               string        // channel to listen for tasks of type TaskType
	DoneTopic             string        // topic to return a done task
	FailRate              int           // int between 0-100 representing a percent
	Dur                   time.Duration // how long the task will take to finish successfully
	DurVariance           time.Duration // random adjustment to the Dur value

}

// NsqdHostsString will set Options.NsqdHosts from a comma
// separated string of hosts.
func (c *options) NsqdHostsString(hosts string) {
	c.NsqdHosts = strings.Split(hosts, ",")
}

// DurString will parse the 'dur' string and attempt to
// convert it to a duration using time.ParseDuration and assign
// that value to c.Dur.
func (c *options) DurString(dur string) error {
	d, err := time.ParseDuration(dur)
	if err != nil {
		return err
	}
	c.Dur = d

	return nil
}

func (c *options) Validate() error {
	// must have a task type
	if c.TaskType == "" {
		return errors.New("required: type flag")
	}

	return nil
}

// DurVarianceString will parse the 'dur' string and attempt to
// convert it to a duration using time.ParseDuration and assign
// that value to c.DurVariance.
func (c *options) DurVarianceString(dur string) error {
	d, err := time.ParseDuration(dur)
	if err != nil {
		return err
	}
	c.DurVariance = d

	return nil
}

// loadAppOptions loads the applications
// options and sets those options to the
// global appOpt variable.
func loadAppOptions() {
	flag.Parse()

	// load config
	opt := newOptions()
	opt.Bus = *tskBus
	opt.InBus = *inBus
	opt.OutBus = *outBus
	opt.InFile = *inFile
	opt.OutFile = *outFile
	opt.TaskType = *tskType
	opt.Topic = *tskType // default topic
	if *topic != "" {
		opt.Topic = *topic
	}
	opt.Channel = *tskType // default channel
	if *channel != "" {
		opt.Channel = *channel
	}
	opt.DoneTopic = *doneTopic
	opt.FailRate = *failRate
	opt.NsqdHostsString(*nsqdHosts)
	opt.DurString(*dur)
	opt.DurVarianceString(*durVariance)
	opt.MaxInProgress = *maxInProgress
	opt.WorkerTimeout = *workerTimeout
	opt.LifetimeMaxWorkers = *lifetimeMaxWorkers

	appOpt = opt
}
