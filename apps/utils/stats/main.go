package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
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
	Port        int           `toml:"http_port"`
	Bus         bus.Options   `toml:"bus"`
	PollPeriod  time.Duration `toml:"poll_period" commented:"true"`
	consumers   []bus.Consumer
	starttime   time.Time
	TopicPrefix string `toml:"topic_prefix" comment:"(optional) topic prefix filter. Can be used to only connect to topic with a certain prefix"`
	topics      map[string]*stat
	SlackURL    string `toml:"slack" comment:"slack URL for sending"`
}

func main() {
	log.SetFlags(log.Lshortfile | log.Ltime)

	registerMetrics()

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
			Bus:          "nsq",
			InChannel:    "stats",
			LookupdHosts: []string{"localhost:4160"},
		},
		consumers:  make([]bus.Consumer, 0),
		starttime:  time.Now(),
		PollPeriod: time.Minute,
		topics:     make(map[string]*stat),
	}
}

// Start the stats app
func (a *app) Start() {
	log.Printf("starting server on port %d", a.Port)
	http.HandleFunc("/", a.handler)
	go http.ListenAndServe(":8081", metricsHandler())
	go http.ListenAndServe(":"+strconv.Itoa(a.Port), nil)
	ctx, cancel := context.WithCancel(context.Background())
	go a.run(ctx)
	sigChan := make(chan os.Signal) // app signal handling
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	select {
	case <-sigChan:
		fmt.Println("closing...")
		cancel()
		go func() {
			time.Sleep(5 * time.Second)
			log.Fatal("forced shutdown")
		}()
		a.Stop()
	}
}

// run scans all topics on the bus and creates new consumers for new topics
func (a *app) run(ctx context.Context) {
	outmsg := make(chan *task.Task, 2)
	go a.processMessages(ctx, outmsg)
	a.processTopics(ctx, outmsg)
}

func (a *app) processMessages(ctx context.Context, out chan *task.Task) {
	for {
		select {
		case <-ctx.Done():
			return
		case tsk := <-out:
			log.Println(tsk.JSONString())
			sts := a.topics[tsk.Type]
			if tsk.Result != "" {
				sts.DoneTask(*tsk)
				continue
			}
			sts.NewTask(*tsk)
		}
	}
}

func (a *app) processTopics(ctx context.Context, msgOut chan *task.Task) {
	scan := make(chan time.Time, 1)
	ticker := time.NewTicker(a.PollPeriod)
	go func() {
		for t := range ticker.C {
			scan <- t
		}
	}()
	scan <- time.Now() // used to create a do while loop. (run at startup without waiting for poll period)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-scan: // process the topics
			topics, err := bus.Topics(&a.Bus)
			if err != nil {
				log.Println(err)
				continue
			}
			for _, t := range topics {
				if !strings.HasPrefix(t, a.TopicPrefix) {
					continue
				}
				// create new consumer and stat
				if _, found := a.topics[t]; !found {
					bOpt := a.Bus
					bOpt.InTopic = t
					bOpt.InChannel = t + "-stats"
					log.Println(t, t+"-stats")
					c, err := bus.NewConsumer(&bOpt)
					if err != nil {
						log.Println("consumer create error", err)
						continue
					}
					go readConsumer(c, msgOut)
					log.Printf("connecting to %s", t)
					a.topics[t] = newStat()
				}
			}
		}
	}
}

// Read bus messages
func readConsumer(c bus.Consumer, out chan *task.Task) {
	for msg, done, err := c.Msg(); !done; msg, done, err = c.Msg() {
		if err != nil {
			log.Println(err)
			// todo: should we close the consumer/stat object on error?
			break
		}
		if done {
			break
		}
		t, err := task.NewFromBytes(msg)
		if err != nil {
			log.Printf("invalid task message %s: %s", string(msg), err)
			continue
		}
		out <- t
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

// Stop the app, disconnect all consumer connections
func (a *app) Stop() {
	wg := sync.WaitGroup{}
	for _, c := range a.consumers {
		wg.Add(1)
		go func(c bus.Consumer) {
			if err := c.Stop(); err != nil {
				log.Println(err)
			}
			wg.Done()
		}(c)
	}

	wg.Wait()
}
