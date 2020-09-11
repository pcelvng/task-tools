package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hydronica/toml"
	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/workflow"
)

const (
	tFormat     = "2006-01-02T15"
	dFormat     = "2006-01-02"
	defTemplate = "{yyyy}-{mm}-{dd}T{hh}"
)

type options struct {
	Workflow string       `toml:"workflow"`
	File     file.Options `toml:"file"`
	Bus      bus.Options  `toml:"bus"`
	cache    *workflow.Cache

	start time.Time // start of backload
	end   time.Time // end of backload

	taskType     string
	taskTemplate string

	everyXHours int    // default skips 0 hours aka does all hours. Will always at least create a task for the start date.
	onHours     []bool // each key represents the hour and bool is if that value is turned on. (not specified means all hours are ON)
	offHours    []bool // each key represents the hour and bool is if that value is turned off.
	meta        string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

type flags struct {
	version bool
	config  string

	taskType     string
	job          string
	taskTemplate string
	bus          string

	at    string
	from  string
	to    string
	daily bool

	everyXHours int
	onHours     string
	offHours    string
}

func run() error {
	// setup flags
	f := flags{}
	version := flag.Bool("version", false, "show version")
	flag.BoolVar(version, "v", false, "show version")
	flag.StringVar(&f.config, "c", "", "(optional config path)")

	flag.StringVar(&f.taskType, "type", "", "REQUIRED; the task type")
	flag.StringVar(&f.taskType, "t", "", "alias of 'type'")
	flag.StringVar(&f.job, "job", "", "(optional: with config) workflow job")
	flag.StringVar(&f.taskTemplate, "template", defTemplate, "task template")
	flag.StringVar(&f.bus, "bus", "stdout", "one of 'stdout', 'file', 'nsq', 'pubsub'")
	flag.StringVar(&f.bus, "b", "", "alias of 'bus'")

	flag.StringVar(&f.at, "at", "", "run once for a specific time. format 'yyyy-mm-ddThh' (example: '2017-01-03T01')")
	flag.StringVar(&f.from, "from", "now", "format 'yyyy-mm-ddThh' (example: '2017-01-03T01'). Allows a special keyword 'now'.")
	flag.StringVar(&f.to, "to", "", "same format as 'from'; if not specified, will run the one hour specified by from. Allows special keyword 'now'.")
	flag.BoolVar(&f.daily, "daily", false, "sets hour to 00 and populates every 24 hours")

	flag.IntVar(&f.everyXHours, "every-x-hours", 0, "will generate a task every x hours. Includes the first hour. Can be combined with 'on-hours' and 'off-hours' fions.")
	flag.StringVar(&f.onHours, "on-hours", "", "comma separated list of hours to indicate which hours of a day to back-load during a 24 period (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. Example: '0,4,15' - will only generate tasks on hours 0, 4 and 15")
	flag.StringVar(&f.offHours, "off-hours", "", "comma separated list of hours to indicate which hours of a day to NOT create a task (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. If used will trump 'on-hours' values. Example: '2,9,16' - will generate tasks for all hours except 2, 9 and 16.")

	flag.Parse()
	if *version {
		fmt.Println(tools.String())
		os.Exit(0)
	}

	// app config
	appConf, err := loadOptions(f)
	if err != nil {
		return err
	}

	// backloader
	bl, err := newBackloader(appConf)
	if err != nil {
		return err
	}

	doneChan := make(chan error)
	go func() {
		_, err := bl.backload()
		doneChan <- err
	}()

	// signal handling - capture signal early.
	var sigChan = make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	select {
	case blErr := <-doneChan:
		return blErr
	case <-sigChan:
		if err := bl.stop(); err != nil {
			return err
		}
	}
	return nil
}

func loadOptions(f flags) (*options, error) {
	opt := &options{
		taskType:     f.taskType,
		taskTemplate: f.taskTemplate,
		everyXHours:  f.everyXHours,
	}

	if f.config != "" {
		_, err := toml.DecodeFile(f.config, opt)
		if err != nil {
			return nil, err
		}
		opt.cache, err = workflow.New(opt.Workflow, &opt.File)
		if err != nil {
			return nil, err
		}

		opt.Bus.Bus = "" // don't use bus info from the config force using the flag
		opt.Bus.OutBus = ""
	}

	// setup bus URI from the flag
	bOpts := busOptions{}
	if err := uri.Unmarshal(f.bus, &bOpts); err != nil {
		return nil, err
	}
	if bOpts.Bus == "" {
		bOpts.Bus = bOpts.Path
	}

	opt.Bus.OutBus = bOpts.Bus
	if len(bOpts.Hosts) > 0 {
		opt.Bus.LookupdHosts = bOpts.Hosts
	}
	if bOpts.ProjectID != "" {
		opt.Bus.ProjectID = bOpts.ProjectID
	}
	if bOpts.JSONAuth != "" {
		opt.Bus.JSONAuth = bOpts.JSONAuth
	}

	// populate template
	if f.taskTemplate != defTemplate {
		opt.taskTemplate = f.taskTemplate
	} else if opt.cache != nil {
		w := opt.cache.Search(opt.taskType, f.job)
		if w == "" {
			return nil, fmt.Errorf("no workflow found for %s:%s", opt.taskType, f.job)
		}
		opt.meta = "workflow=" + w

		if f.job != "" {
			opt.meta += "&job=" + f.job
		}
		tsk := task.Task{Type: opt.taskType, Meta: opt.meta}
		if p := opt.cache.Get(tsk); !p.IsEmpty() {
			opt.taskTemplate = p.Template
		}
	}

	if err := opt.setOnHours(f.onHours); err != nil {
		return nil, err
	}

	if err := opt.setOffHours(f.offHours); err != nil {
		return nil, err
	}
	if f.daily {
		opt.setOnHours("0")
		opt.setOffHours("")
	}

	if f.at != "" {
		f.from = f.at
		f.to = f.at
	}

	now := time.Now().Format(tFormat) // 2017-01-03T01
	if f.from == "now" {
		f.from = now
	}

	if f.to == "now" {
		f.to = now
	}

	if err := opt.dateRangeStrings(f.from, f.to); err != nil {
		return nil, err
	}

	return opt, opt.validate()
}

func (c *options) validate() error {
	// TaskType is required
	if c.taskType == "" {
		return errors.New("flag '-type' or '-t' required")
	}

	if c.everyXHours < 0 {
		return errors.New("flag 'every-x-hours` must not be negative")
	}

	return nil
}

type busOptions struct {
	Bus       string   `uri:"scheme"`
	Path      string   `uri:"path"`
	Hosts     []string `uri:"host"`
	ProjectID string   `uri:"project"`
	JSONAuth  string   `uri:"jsonauth"`
}

// setOnHours will parse onHours string and set
// OnHours value.
func (c *options) setOnHours(onHours string) error {
	hrs, err := parseHours(onHours)
	if err != nil {
		return err
	}
	c.onHours = hrs
	return nil
}

// setOffHours will parse onHours string and set
// OnHours value.
func (c *options) setOffHours(offHours string) error {
	hrs, err := parseHours(offHours)
	if err != nil {
		return err
	}

	c.offHours = hrs
	return nil
}

func parseHours(hrsStr string) (hrs []bool, err error) {
	// make hrs exactly 24 slots
	hrs = make([]bool, 24)

	if hrsStr == "" {
		return hrs, err
	}

	// basic sanitation - remove spaces
	hrsStr = strings.Replace(hrsStr, " ", "", -1)

	// convert, sort, de-duplicate
	for _, hour := range strings.Split(hrsStr, ",") {
		hr, err := strconv.Atoi(hour)
		if err != nil {
			return hrs, errors.New(
				fmt.Sprintf("invalid hour value '%v'", hour))
		}
		if 0 <= hr && hr <= 23 {
			hrs[hr] = true
		} else {
			return hrs, errors.New(
				fmt.Sprintf("invalid hour value '%v' must be int between 0 and 23", hour))
		}
	}

	return hrs, nil
}

func (c *options) dateRangeStrings(start, end string) error {
	// parse start
	s, err := time.Parse(tFormat, start)
	if err != nil {
		if s, err = time.Parse(dFormat, start); err != nil {
			return err
		}
	}

	// truncate to hour and assign
	c.start = s.Truncate(time.Hour)

	// start and end are equal if end not provided
	if end == "" {
		c.end = c.start
		return nil
	}

	// parse end (if provided)
	e, err := time.Parse(tFormat, end)
	if err != nil {
		if e, err = time.Parse(dFormat, end); err != nil {
			return err
		}
	}

	// round to hour and assign
	c.end = e.Truncate(time.Hour)
	return nil
}
