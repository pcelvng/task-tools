package main

import (
	"log"
	"sync"
	"time"

	"github.com/jbsmith7741/go-tools/appenderr"

	nsq "github.com/nsqio/go-nsq"
	"github.com/pcelvng/task-tools/file"
)

type Logger struct {
	mu       sync.Mutex
	topic    string
	consumer *nsq.Consumer
	writers  []file.Writer
	Messages int
}

func newlog(topic string, c *nsq.Consumer) *Logger {
	return &Logger{
		topic:    topic,
		consumer: c,
		writers:  make([]file.Writer, 0),
	}
}

func (l *Logger) CreateWriters(opts *file.Options, destinations []string) error {
	errs := appenderr.New()

	// create new writers
	writers := make([]file.Writer, 0)
	for _, d := range destinations {
		path := Parse(d, l.topic, time.Now())
		w, err := file.NewWriter(path, opts)
		if err != nil {
			errs.Add(err)
			continue
		}
		writers = append(writers, w)
	}

	// close and reset open writers
	l.mu.Lock()
	for _, w := range l.writers {
		errs.Add(w.Close())
	}
	l.writers = writers
	l.mu.Unlock()
	return errs.ErrOrNil()
}

func (l *Logger) HandleMessage(msg *nsq.Message) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range l.writers {
		// write the message to each writer
		err := l.writers[i].WriteLine(msg.Body)
		if err != nil {
			log.Println("error writing message", l.writers[i], msg.Body)
		}
	}
	l.Messages++
	return nil
}
