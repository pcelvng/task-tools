package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

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

var (
	appName     = "logger"
	description = `logger is a utility to log nsq messages from all found topics to all destination templates`
)

func main() {
	app := New()
	bootstrap.NewUtility("logger", app).
		Description(`app that writes all topic messages to various destinations`).
		Version(tools.String()).
		Initialize()

	app.Start()
}

func (a *app) Start() {
	log.Printf("starting server on port %d", a.StatusPort)
	http.HandleFunc("/", a.handler)
	go http.ListenAndServe(":"+strconv.Itoa(a.StatusPort), nil)

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

func (a *app) newConsumer(topic string, c *nsq.Consumer) {
	log.Printf("connecting to %s", topic)
	a.consumers = append(a.consumers, c)

	if _, found := a.topics[topic]; found {
		log.Printf("duplicate topic consumer created %s", topic)
		return
	}
	l := newlog(topic, c)

	for _, t := range a.DestTemplates {
		pth := tmpl.Parse(t, time.Now())
		w, err := file.NewWriter(pth, &a.WriteOptions)
		if err != nil {
			log.Println("cannot start new writer [", pth, "] ", err)
		}
		l.writers = append(l.writers, w)
	}

	c.AddHandler(l)
	if err := c.ConnectToNSQLookupds(a.LookupdHosts); err != nil {
		log.Println(err)
	}
	a.topics[topic] = l
}

func (a *app) handler(w http.ResponseWriter, req *http.Request) {
	//v := req.URL.Query()
	for _, lm := range a.topics {
		w.Write(lm.Message())
	}
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
		return errors.New("At least one LookupD host is needed for nsq")
	}

	if len(a.DestTemplates) == 0 {
		return errors.New("At least one destination template is required")
	}

	return nil
}
