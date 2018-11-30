package main

import (
	"log"
	"net/http"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task/bus"
)

const (
	tasktype     = "http"
	defaultTopic = "batcher"
	defaultPort  = "8080"
	description  = `http creates a batcher task based on the http request.
	Values can be provided in a http json body or be provided as uri params.
	
# http request variables ‼(PLEASE NOTE ~ underscores for json body request, dashes for uri query params)‼ 💣

task_type	- the task type for the new tasks (REQUIRED)
topic	    - overrides task-type as the default topic
from 		- the start time of the first task to be created format RFC 3339 YYYY-MM-DDTHH:MM:SSZ (REQUIRED)
template 	- the template used to generated the batch task(s)

*** pick a duration modifier *** 
	to - the end time of the last task to be created format RFC 3339 YYYY-MM-DDTHH:MM:SSZ (takes presidence over for value)
	for - the duration that should be run starting at from (ignored if to value is provided)

every-x-hours - will generate a task every x hours. Includes the first hour. Can be combined with 'on-hours' and 'off-hours' options.
on-hours      - comma separated list of hours to back-load during a 24 period (each value must be between 0-23). 
off-hours     - comma separated list of hours to  NOT create a task (each value must be between 0-23). 
	Note: overrides 'on-hours' values. 

dest-template - task destination template (may have to build a registry for these)

Examples:
  curl -v -X POST -d '{"task-type":"batcher","every-x-hours":"1","from":"2018-05-01T00:00:00Z"}' 'localhost:{http_port}/path/is/ignored/'
  curl -v -X GET 'localhost:{http_port}/path/is/ignored/?task-type=example-task&from=2018-05-01T00:00:00Z'`
)

type httpMaster struct {
	HttpPort string `toml:"http_port"`

	Bus *bus.Options `toml:"bus"`

	producer  bus.Producer
	Templates []template        `toml:"template" comment:"list of templates (name=[\"infoString\"])"`
	Apps      map[string]string `comment:"ip address and status ports of apps (appname=localhost:1234)"`
}

type template struct {
	Name  string
	Topic string
	Info  string
}

func main() {
	tm := newOptions()
	bootstrap.NewUtility(tasktype, tm).
		Version(tools.String()).Description(description).Initialize()

	tm.producer, _ = bus.NewProducer(tm.Bus)

	http.HandleFunc("/batch", tm.handleBatch)
	http.HandleFunc("/status", tm.handleStatus)
	log.Println("starting http server on port", tm.HttpPort)
	log.Print(http.ListenAndServe(":"+tm.HttpPort, nil))

}
func newOptions() *httpMaster {
	return &httpMaster{
		HttpPort: defaultPort,
		Bus:      bus.NewOptions("nop"),
		Apps:     make(map[string]string),
		//Template: make(map[string][]template),
		Templates: make([]template, 0),
	}
}
