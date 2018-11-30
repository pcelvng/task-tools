package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jbsmith7741/go-tools/appenderr"

	"github.com/pcelvng/task-tools/tmpl"

	nsq "github.com/nsqio/go-nsq"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/consumer"
	"github.com/pcelvng/task-tools/file"
)

type app struct {
	StatusPort    int           `toml:"status_port"`
	DestTemplates []string      `toml:"dest_templates" comment:"list of locations to save topic output"`
	Bus           string        `toml:"bus" comment:"the bus type ie:nsq, kafka"`
	LookupdHosts  []string      `toml:"lookupd_hosts" comment:"host names of nsq lookupd servers"`
	Channel       string        `toml:"channel" comment:"read nsq channel default logger"`
	PollPeriod    time.Duration `toml:"poll_period" comment:"the time between refresh on the topic list"`
	WriteOptions  file.Options  `toml:"write_options"`

	consumers []*nsq.Consumer
	topics    map[string]*Logger
}

func New() *app {
	a := &app{
		StatusPort:    8080,
		Bus:           "nsq",
		Channel:       "logger",
		PollPeriod:    time.Minute,
		DestTemplates: make([]string, 0),
		consumers:     make([]*nsq.Consumer, 0),
		topics:        make(map[string]*Logger),
	}

	return a
}

const (
	description = `logger is a utility to log nsq messages from all found topics to all destination templates`
)

func main() {
	app := New()
	bootstrap.NewUtility("logger", app).
		Description(description).
		Version(tools.String()).
		AddInfo(app.Info, app.StatusPort).
		Initialize()

	app.Start()
}

func (a *app) Info() interface{} {
	data := make(map[string]int)
	for name, topic := range a.topics {
		data[name] = topic.Messages
	}
	return data
}

func (a *app) Start() {
	go a.RotateLogs()
	consumer.DiscoverTopics(a.newConsumer, a.Channel, a.PollPeriod, a.LookupdHosts)

	sigChan := make(chan os.Signal) // app signal handling
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	select {
	case <-sigChan:
		fmt.Println("closing...")
		go func() {
			time.Sleep(5 * time.Second)
			log.Fatal("forced shutdown")
		}()
		a.Stop()
	}
}
func (a *app) RotateLogs() {
	for ; ; time.Sleep(nextHour(time.Now())) {
		if err := a.rotateWriters(time.Now()); err != nil {
			log.Println(err)
			a.Stop()
		}
	}
}

func nextHour(t time.Time) time.Duration {
	h := t.Truncate(time.Hour).Add(59*time.Minute + 50*time.Second)
	d := h.Sub(t.Round(time.Second))
	if d <= 0 {
		d += time.Hour
	}
	return d
}

func (a *app) rotateWriters(tm time.Time) error {
	fmt.Println("files rotate", len(a.topics))
	errs := appenderr.New()
	for _, topic := range a.topics {
		errs.Add(topic.CreateWriters(&a.WriteOptions, a.DestTemplates))
	}
	return errs.ErrOrNil()
}

func Parse(path, topic string, t time.Time) string {
	path = strings.Replace(path, "{topic}", topic, -1)
	path = strings.Replace(path, "{TOPIC}", topic, -1)
	return tmpl.Parse(path, t)
}

func (a *app) newConsumer(topic string, c *nsq.Consumer) {
	log.Printf("connecting to %s", topic)
	a.consumers = append(a.consumers, c)

	if _, found := a.topics[topic]; found {
		log.Printf("duplicate topic consumer created %s", topic)
		return
	}
	l := newlog(topic, c)

	c.AddHandler(l)
	if err := l.CreateWriters(&a.WriteOptions, a.DestTemplates); err != nil {
		log.Println(err)
	}
	if err := c.ConnectToNSQLookupds(a.LookupdHosts); err != nil {
		log.Println(err)
	}
	a.topics[topic] = l
}

func (a *app) Stop() {
	wg := sync.WaitGroup{}
	for i := range a.consumers {
		c := a.consumers[i]
		wg.Add(1)
		go func() {
			c.Stop()
			<-c.StopChan
			wg.Done()
		}()
	}

	wg.Wait()
}

func (a *app) Validate() error {
	if a.Bus == "nsq" && len(a.LookupdHosts) == 0 {
		return errors.New("at least one LookupD host is needed for nsq")
	}

	if len(a.DestTemplates) == 0 {
		return errors.New("at least one destination template is required")
	}

	return nil
}
