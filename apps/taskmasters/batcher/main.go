package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/BurntSushi/toml"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task/bus"
	ptoml "gopkg.in/pelletier/go-toml.v1"
)

const (
	tasktype    = "batcher"
	description = `batcher creates a set of batch to be passed on to downstream processes

	type - the downstream task type (required)
	# - the downstream workers info template (required)
	from - the start time of the first task to be created (required)
	*** pick a duration modifier *** 
		to - the end time of the last task to be created
		for - the duration that should be run 

Example:
{"type":"batcher", "info":"topic?from=2006-01-02T15&for=-24h#s3://path/{yyyy}/{mm}/{dd}/{hh}.json.gz?options"}

{"type":"batcher", "info":"?task=topic&from=2006-01-02T15&for=-24h#template=s3://path/{yyyy}/{mm}/{dd}/{hh}.json.gz?options"}
`
)

var config = flag.String("c", "", "path to config file")
var version = flag.Bool("v", false, "show the version")
var genConfig = flag.Bool("g", false, "generate the config file")

type options struct {
	Bus bus.Options `toml:"bus"`
}

func main() {
	opt := &options{
		Bus: *bus.NewOptions(""),
	}
	flag.Parse()

	if *version {
		log.Println(tools.String())
		os.Exit(0)
	}
	if *genConfig {
		b, _ := ptoml.Marshal(*opt)
		fmt.Println(string(b))
		os.Exit(0)
	}
	if *config == "" {
		log.Fatal("config file is required")
	}
	toml.DecodeFile(*config, opt)

	tm, err := New(opt.Bus)
	if err != nil {
		log.Fatal(err)
	}
	tm.Start()
}
