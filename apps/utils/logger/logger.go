package main

import (
	"log"
	"sync"
	"time"

	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pcelvng/task/bus"

	"github.com/pcelvng/task-tools/file"
)

type Logger struct {
	mu       sync.Mutex
	topic    string
	consumer bus.Consumer
	writer   file.Writer
	Messages int
	done     chan struct{}
}

func newlog(topic string, c bus.Consumer) *Logger {
	l := &Logger{
		topic:    topic,
		consumer: c,
	}
	go l.Read()
	return l
}

func (l *Logger) Stop() {
	l.consumer.Stop()
	<-l.done
}

func (l *Logger) CreateWriters(opts *file.Options, destination string) error {
	errs := appenderr.New()

	// create new writer
	path := Parse(destination, l.topic, time.Now())
	w, err := file.NewWriter(path, opts)
	if err != nil {
		errs.Add(err)
	}

	// close and reset open writer
	l.mu.Lock()
	// the previous writer should be closed or aborted before the new writer are set
	if l.writer != nil {
		if l.writer.Stats().ByteCnt > 0 {
			errs.Add(l.writer.Close())
		} else {
			errs.Add(l.writer.Abort())
		}
	}
	// set new writer
	l.writer = w
	l.mu.Unlock()
	return errs.ErrOrNil()
}

func (l *Logger) Read() {
	for msg, done, err := l.consumer.Msg(); !done; msg, done, err = l.consumer.Msg() {
		if err != nil {
			log.Println(err)
			break
		}
		if done {
			break
		}
		l.mu.Lock()
		l.writer.WriteLine(msg)
		l.mu.Unlock()
	}

	if l.writer.Stats().ByteCnt > 0 {
		l.writer.Close()
	} else {
		l.writer.Abort()
	}
	l.done <- struct{}{}
}
