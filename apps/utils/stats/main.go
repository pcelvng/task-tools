package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jbsmith7741/uri"
	nsq "github.com/nsqio/go-nsq"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/consumer"
	"github.com/pcelvng/task/bus"
)

const (
	desc = `stats connects to all nsq topics to gather statistics on each topic
which can be queried through http get requests.
By default all topics are shown, but can be selected using query params

curl localhost:8080?topic=done
curl localhost:8080?topic=task1,task2
or
curl localhost:8080?topic=task1&topic=task2`
)

type app struct {
	Port       int           `toml:"http_port"`
	Bus        bus.Options   `toml:"bus"`
	PollPeriod time.Duration `toml:"poll_period" commented:"true"`
	consumers  []*nsq.Consumer
	starttime  time.Time
	topics     map[string]*stat
}

func main() {
	log.SetFlags(log.Lshortfile)

	app := New()
	bootstrap.NewUtility("stats", app).
		Description(desc).
		Version(tools.String()).
		Initialize()

	app.Start()
}

func New() *app {
	return &app{
		Port: 8080,
		Bus: bus.Options{
			Bus:       "nsq",
			InChannel: "stats",
		},
		starttime:  time.Now(),
		PollPeriod: time.Minute,
		consumers:  make([]*nsq.Consumer, 0),
		topics:     make(map[string]*stat),
	}
}

// Start the stats app
func (a *app) Start() {
	consumer.DiscoverTopics(a.newConsumer, a.Bus.InChannel, a.PollPeriod, a.Bus.LookupdHosts)

	log.Printf("starting server on port %d", a.Port)
	http.HandleFunc("/", a.handler)
	go http.ListenAndServe(":"+strconv.Itoa(a.Port), nil)

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

// handler for http requests
func (a *app) handler(w http.ResponseWriter, req *http.Request) {
	v := struct {
		Topic []string `uri:"topic"`
	}{}
	uri.Unmarshal(req.URL.String(), &v)

	if len(v.Topic) == 0 {
		for name := range a.topics {
			v.Topic = append(v.Topic, name)
		}
		sort.Sort(sort.StringSlice(v.Topic))
	}
	s := a.Message(v.Topic...)
	w.Write([]byte(s))
}

// Stop the app, disconnect all nsq connection
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

// Message creates a string of the statistics of the selected topics.
// no topics provided will show all topics
func (a *app) Message(topics ...string) string {
	s := fmt.Sprintf("uptime: %v\n", time.Now().Sub(a.starttime))
	if len(topics) == 0 {
		for name, topic := range a.topics {
			s += name + "\n" + topic.Details() + "\n"
		}
		return s
	}
	for _, t := range topics {
		topic, found := a.topics[t]
		if !found {
			continue
		}
		s += t + "\n" + topic.Details() + "\n"
	}
	return s
}

func (a *app) newConsumer(topic string, c *nsq.Consumer) {
	log.Printf("connecting to %s", topic)
	a.consumers = append(a.consumers, c)
	if topic == "done" {
		c.AddHandler(a)
		if err := c.ConnectToNSQLookupds(a.Bus.LookupdHosts); err != nil {
			log.Println(err)
		}
		return
	}

	if _, found := a.topics[topic]; found {
		log.Printf("duplicate topic consumer created %s", topic)
		return
	}
	s := newStat(c)
	c.AddHandler(s)
	if err := c.ConnectToNSQLookupds(a.Bus.LookupdHosts); err != nil {
		log.Println(err)
	}
	a.topics[topic] = s
}

// HandleMessage handles nsq message as they come in
func (a *app) HandleMessage(msg *nsq.Message) error {
	t, err := task.NewFromBytes(msg.Body)
	if err != nil {
		log.Println("invalid task", err)
		return nil
	}
	for _, s := range a.topics {
		s.DoneTask(*t)
	}
	return nil
}
