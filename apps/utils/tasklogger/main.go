package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/jbsmith7741/go-tools/appenderr"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task/bus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type app struct {
	StatusPort  int           `toml:"status_port"`
	LogPath     string        `toml:"log_path" comment:"destination template path for the logs to be written to"`
	Bus         bus.Options   `toml:"bus"`
	PollPeriod  time.Duration `toml:"poll_period" comment:"refresh time to check current topics"`
	File        file.Options  `toml:"file"`
	RotateFiles time.Duration `toml:"rotate_files" comment:"time between rotation for log files default is an hour (3600000000000 nano seconds)"`
	client      *pubsub.Client
	topics      map[string]*Logger
	TopicPrefix string `toml:"topic_prefix" comment:"(optional) topic prefix filter. Can be used to only connect to topic with a certain prefix"`
}

const (
	description = `logger is a utility to log nsq messages from all found topics to all destination templates`
)

func New() *app {
	a := &app{
		StatusPort: 0,
		Bus: bus.Options{
			Bus:       "pubsub",
			InChannel: "logger",
		},
		PollPeriod: time.Minute,
		LogPath:    "./{TS}-{topic}.json.gz",
		topics:     make(map[string]*Logger),
	}

	return a
}

func main() {
	app := New()
	u := bootstrap.NewUtility("logger", app).
		Description(description).
		Version(tools.String())
	u.Initialize()
	u.AddInfo(app.Info, app.StatusPort)

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
	opts := make([]option.ClientOption, 0)
	if a.Bus.JSONAuth != "" {
		opts = append(opts, option.WithCredentialsFile(a.Bus.JSONAuth))
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := pubsub.NewClient(ctx, a.Bus.ProjectID)
	if err != nil {
		log.Fatal("pubsub connect ", err)
	}
	a.client = client
	go a.RotateLogs()
	go a.UpdateTopics()

	sigChan := make(chan os.Signal) // app signal handling
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	select {
	case <-sigChan:
		fmt.Println("closing...")
		go func() {
			time.Sleep(5 * time.Second)
			log.Fatal("fatal: forced shutdown")
		}()
		a.Stop()
	}
}

func (a *app) UpdateTopics() {
	for _, t := range Topics(a.client) {
		if !strings.HasPrefix(t, a.TopicPrefix) {
			continue
		}
		if _, found := a.topics[t]; !found {
			opts := a.Bus
			opts.InTopic = t
			opts.InChannel = t + "-logger"
			c, err := bus.NewConsumer(&opts)
			if err != nil {
				log.Println("consumer create error", err)
				continue
			}
			log.Printf("connecting to %s", t)
			l := newlog(t, c)
			if err := l.CreateWriters(&a.File, Parse(a.LogPath, t, time.Now())); err != nil {
				log.Fatalf("writer err for %s: %s", t, err)
			}
			a.topics[t] = l
		}
	}
}

func Topics(client *pubsub.Client) []string {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	q := client.Topics(ctx)
	topics := make([]string, 0)
	t, err := q.Next()
	for ; err == nil; t, err = q.Next() {
		topics = append(topics, t.ID())
	}
	if err != iterator.Done {
		log.Println(err)
	}

	return topics
}

func (a *app) RotateLogs() {
	if a.RotateFiles == 0 {
		a.RotateFiles = time.Hour
	}
	for ; ; time.Sleep(a.RotateFiles) {
		if err := a.rotateWriters(time.Now()); err != nil {
			log.Println(err)
			a.Stop()
		}
	}
}

func (a *app) rotateWriters(tm time.Time) error {
	fmt.Println("files rotate", len(a.topics))
	errs := appenderr.New()
	for _, topic := range a.topics {
		errs.Add(topic.CreateWriters(&a.File, a.LogPath))
	}
	return errs.ErrOrNil()
}

func Parse(path, topic string, t time.Time) string {
	path = strings.Replace(path, "{topic}", topic, -1)
	path = strings.Replace(path, "{TOPIC}", topic, -1)
	return tmpl.Parse(path, t)
}

func (a *app) Stop() {
	wg := sync.WaitGroup{}
	for t, l := range a.topics {
		wg.Add(1)
		go func() {
			l.Stop()
			log.Print(t, "stopped")
			wg.Done()
		}()
	}

	wg.Wait()
}

func (a *app) Validate() error {
	if a.Bus.Bus == "nsq" && len(a.Bus.LookupdHosts) == 0 {
		return errors.New("error: at least one lookupd host is needed for nsq")
	}

	if a.Bus.Bus == "pubsub" && a.Bus.ProjectID == "" {
		return errors.New("pubsub project id is required")
	}

	if a.LogPath == "" {
		return errors.New("error: log path is required")
	}
	return nil
}
