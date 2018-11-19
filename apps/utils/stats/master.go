package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	nsq "github.com/nsqio/go-nsq"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/consumer"
	"github.com/pcelvng/task/bus"
)

type master struct {
	Port       int           `toml:"http_port"`
	Bus        bus.Options   `toml:"bus"`
	PollPeriod time.Duration `toml:"poll_period" commented:"true"`

	starttime time.Time
	consumers []*nsq.Consumer
	topics    map[string]*stat
}

func New() *master {
	return &master{
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

func (m *master) Start() {
	consumer.DiscoverTopics(m.newConsumer, m.Bus.InChannel, m.PollPeriod, m.Bus.LookupdHosts)

	sigChan := make(chan os.Signal, 1) // app signal handling
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	select {
	case <-sigChan:
		log.Println("closing...")
		go func() {
			time.Sleep(5 * time.Second)
			log.Fatal("forced shutdown")
		}()
		m.Close()
	}
}

func (m *master) Close() {
	wg := sync.WaitGroup{}
	for i := range m.consumers {
		c := m.consumers[i]
		wg.Add(1)
		go func() {
			c.Stop()
			<-c.StopChan
			wg.Done()
		}()
	}

	wg.Wait()
}

func (m *master) newConsumer(topic string, c *nsq.Consumer) {
	log.Printf("connecting to %s", topic)
	m.consumers = append(m.consumers, c)
	if topic == "done" {
		c.AddHandler(m)
		if err := c.ConnectToNSQLookupds(m.Bus.LookupdHosts); err != nil {
			log.Println(err)
		}
		return
	}

	if _, found := m.topics[topic]; found {
		log.Printf("duplicate topic consumer created %s", topic)
		return
	}
	s := newStat(c)
	c.AddHandler(s)
	if err := c.ConnectToNSQLookupds(m.Bus.LookupdHosts); err != nil {
		log.Println(err)
	}
	m.topics[topic] = s
}

func (m *master) HandleMessage(msg *nsq.Message) error {
	t, err := task.NewFromBytes(msg.Body)
	if err != nil {
		log.Println(err)
		return nil
	}
	for _, s := range m.topics {
		s.DoneTask(t)
	}
	return nil
}
