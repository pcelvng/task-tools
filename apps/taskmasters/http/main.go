package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

const (
	tasktype    = "http"
	description = `http creates a batcher task based on the http request values, query params overwrite json body values, if a body is provided it must be in json format
	info - if provided it sends this value as the info value to the bus, if not provided will send the parsed request values in a uri string
	
	# - config variables
	*** config defaults
	  bus        - stdio
		task_type  - batcher
		http_port  - 8080
	
  # http request variables ‼(PLEASE NOTE ~ underscores for json body request, dashes for uri query params)‼ 💣
	  from - the start time of the first task to be created format RFC 3339 YYYY-MM-DDTHH:MM:SSZ (REQUIRED)
	  *** pick a duration modifier *** 
		  to - the end time of the last task to be created format RFC 3339 YYYY-MM-DDTHH:MM:SSZ (takes presidence over for value)
		  for - the duration that should be run starting at from (ignored if to value is provided)
	  task_type     - the task type for the new tasks (REQUIRED) 
	  every-x-hours - will generate a task every x hours. Includes the first hour. Can be combined with 'on-hours' and 'off-hours' options.
	  on-hours      - comma separated list of hours to indicate which hours of a day to back-load during a 24 period (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. Example: '0,4,15' - will only generate tasks on hours 0, 4 and 15
	  off-hours     - comma separated list of hours to indicate which hours of a day to NOT create a task (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. If used will trump 'on-hours' values. Example: '2,9,16' - will generate tasks for all hours except 2, 9 and 16.
	  topic         - overrides task-type as the default topic
		fragment      - task destination template (may have to build a registry for these)

Examples:
  curl -v -X POST -d '{"task-type":"batcher","every-x-hours":"1","from":"2018-05-01T00:00:00Z"}' 'localhost:{http_port}/path/is/ignored/'
  curl -v -X GET 'localhost:{http_port}/path/is/ignored/?task-type=example-task&from=2018-05-01T00:00:00Z'
`
)

var (
	configPth    = flag.String("config", "config.toml", "relative or absolute file path")
	sigChan      = make(chan os.Signal, 1) // app signal handling
	defaultTopic = "batcher"
	defaultPort  = "8080"
)

type options struct {
	HttpPort string `toml:"http_port"`

	*bus.Options // bus options

	producer bus.Producer
}

func main() {
	err := run()
	if err != nil {
		log.Fatalf("err: %v", err)
	}
}

// run loads the toml config options
// also runs the http server with a catch all route /
func run() error {
	appOpt, err := loadAppOptions()
	if err != nil {
		return fmt.Errorf("toml: '%v'\n", err.Error())
	}

	if appOpt.Bus != "-" {
		appOpt.producer, _ = bus.NewProducer(appOpt.Options)
	}

	http.HandleFunc("/", appOpt.handleRequest)
	log.Println("starting http server on port", appOpt.HttpPort)
	log.Print(http.ListenAndServe(":"+appOpt.HttpPort, nil))
	return nil
}

func newOptions() *options {
	return &options{
		Options:  task.NewBusOptions("nop"),
		HttpPort: defaultPort,
	}
}

// loadAppOptions loads the applications
// options and sets those options to the
// global appOpt variable.
func loadAppOptions() (*options, error) {
	flag.Parse()
	opt := newOptions()

	// parse toml first - override with flag values
	_, err := toml.DecodeFile(*configPth, opt)
	if err != nil {
		return nil, err
	}
	return opt, nil
}
