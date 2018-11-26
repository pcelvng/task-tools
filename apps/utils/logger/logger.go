package main

import (
	"fmt"
	"log"
	"sync"

	nsq "github.com/nsqio/go-nsq"
	"github.com/pcelvng/task-tools/file"
)

type Logger struct {
	mu       sync.Mutex
	topic    string
	consumer *nsq.Consumer
	writers  []file.Writer
	messages int
}

func newlog(topic string, c *nsq.Consumer) *Logger {
	return &Logger{
		topic:    topic,
		consumer: c,
		writers:  make([]file.Writer, 0),
	}
}

func (l *Logger) HandleMessage(msg *nsq.Message) error {
	for i := range l.writers {
		// write the message to each writer
		err := l.writers[i].WriteLine(msg.Body)
		if err != nil {
			log.Println("error writing message", l.writers[i], msg.Body)
		}
	}
	l.messages++
	return nil
}

func (l *Logger) Message() []byte {
	return []byte(fmt.Sprintf(`{"topic":"%s","messages":%d}`, l.topic, l.messages))
}
