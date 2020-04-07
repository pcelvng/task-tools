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
	"github.com/pcelvng/task/bus"

	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/workflow"
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
	appConf, err := loadOptions()
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

var (
	tskType = flag.String("type", "", "REQUIRED; the task type")
	at      = flag.String("at", "", "run once for a specific time. format 'yyyy-mm-ddThh' (example: '2017-01-03T01')")
	from    = flag.String("from", "now", "format 'yyyy-mm-ddThh' (example: '2017-01-03T01'). Allows a special keyword 'now'.")
	to      = flag.String("to", "", "same format as 'from'; if not specified, will run the one hour specified by from. Allows special keyword 'now'.")
	outBus  = flag.String("bus", "stdout", "one of 'stdout', 'file', 'nsq', 'pubsub'")
	//	nsqdHosts   = flag.String("nsqd-hosts", "localhost:4150", "comma-separated list of nsqd hosts with port")
	template    = flag.String("template", "{yyyy}-{mm}-{dd}T{hh}:00", "task template")
	daily       = flag.Bool("daily", false, "sets hour to 00 and populates every 24 hours")
	everyXHours = flag.Uint("every-x-hours", 0, "will generate a task every x hours. Includes the first hour. Can be combined with 'on-hours' and 'off-hours' options.")
	onHours     = flag.String("on-hours", "", "comma separated list of hours to indicate which hours of a day to back-load during a 24 period (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. Example: '0,4,15' - will only generate tasks on hours 0, 4 and 15")
	offHours    = flag.String("off-hours", "", "comma separated list of hours to indicate which hours of a day to NOT create a task (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. If used will trump 'on-hours' values. Example: '2,9,16' - will generate tasks for all hours except 2, 9 and 16.")
	version     = flag.Bool("version", false, "show version")
	config      = flag.String("c", "", "(optional config path)")
	dFmt        = "2006-01-02T15"
)

func init() {
	flag.StringVar(tskType, "t", "", "alias of 'type'")
	flag.StringVar(outBus, "b", "", "alias of 'bus'")
	flag.BoolVar(version, "v", false, "show version")
}

type Config struct {
	Workflow string       `toml:"workflow"`
	File     file.Options `toml:"file"`
	cache    *workflow.Cache
}

func newOptions() *options {
	return &options{
		Options: bus.NewOptions(""),
	}
}

type busOptions struct {
	Bus       string   `uri:"scheme"`
	Hosts     []string `uri:"host"`
	ProjectID string   `uri:"project"`
	JSONAuth  string   `uri:"jsonauth"`
}

type options struct {
	*bus.Options

	Start time.Time // start of backload
	End   time.Time // end of backload

	TaskType     string
	TaskTemplate string

	EveryXHours int    // default skips 0 hours aka does all hours. Will always at least create a task for the start date.
	OnHours     []bool // each key represents the hour and bool is if that value is turned on. (not specified means all hours are ON)
	OffHours    []bool // each key represents the hour and bool is if that value is turned off.
}

// setOnHours will parse onHours string and set
// OnHours value.
func (c *options) setOnHours(onHours string) error {
	hrs, err := parseHours(onHours)
	if err != nil {
		return err
	}

	c.OnHours = hrs
	return nil
}

// setOffHours will parse onHours string and set
// OnHours value.
func (c *options) setOffHours(offHours string) error {
	hrs, err := parseHours(offHours)
	if err != nil {
		return err
	}

	c.OffHours = hrs
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
	s, err := time.Parse(dFmt, start)
	if err != nil {
		log.Println("cannot parse start")
		return err
	}

	// truncate to hour and assign
	c.Start = s.Truncate(time.Hour)

	// start and end are equal if end not provided
	if end == "" {
		c.End = c.Start
		return nil
	}

	// parse end (if provided)
	e, err := time.Parse(dFmt, end)
	if err != nil {
		return err
	}

	// round to hour and assign
	c.End = e.Truncate(time.Hour)

	return nil
}

func (c *options) validate() error {
	// TaskType is required
	if c.TaskType == "" {
		return errors.New("flag '-type' or '-t' required")
	}

	return nil
}

func loadOptions() (*options, error) {
	if *version {
		tools.String()
		os.Exit(0)
	}

	var fConf *Config
	if *config != "" {
		fConf = &Config{}
		if _, err := toml.DecodeFile(*config, fConf); err != nil {
			return nil, err
		}
		c, err := workflow.New(fConf.Workflow, &fConf.File)
		if err != nil {
			return nil, err
		}
	}
	ops := busOptions{}
	c := newOptions()

	if err := uri.Unmarshal(*outBus, &ops); err != nil {
		return nil, err
	}
	c.OutBus = ops.Bus
	c.LookupdHosts = ops.Hosts
	c.ProjectID = ops.ProjectID
	c.JSONAuth = ops.JSONAuth

	// load config
	c.TaskType = *tskType

	// populate template
	c.TaskTemplate = *template
	if fConf != nil {
		fConf.cache.Get(c.TaskType)
	}
	c.EveryXHours = int(*everyXHours)

	if err := c.setOnHours(*onHours); err != nil {
		return nil, err
	}

	if err := c.setOffHours(*offHours); err != nil {
		return nil, err
	}
	if *daily {
		c.setOnHours("0")
		c.setOffHours("")
	}
	from := *from
	to := *to
	if *at != "" {
		from = *at
		to = *at
	}

	now := time.Now().Format(dFmt) // 2017-01-03T01
	if from == "now" {
		from = now
	}

	if to == "now" {
		to = now
	}

	if err := c.dateRangeStrings(from, to); err != nil {
		return nil, err
	}

	return c, nil
}
