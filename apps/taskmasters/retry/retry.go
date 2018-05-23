package main

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

func newRetryer(conf *options) (*retryer, error) {
	if len(conf.RetryRules) == 0 {
		return nil, errors.New("no retry rules specified")
	}

	// map over done topic and channel for consumer
	conf.Options.InTopic = conf.DoneTopic
	conf.Options.InChannel = conf.DoneChannel

	// make consumer
	c, err := bus.NewConsumer(conf.Options)
	if err != nil {
		return nil, err
	}

	// make producer
	p, err := bus.NewProducer(conf.Options)
	if err != nil {
		return nil, err
	}

	r := &retryer{
		conf:       conf,
		consumer:   c,
		producer:   p,
		rules:      conf.RetryRules,
		closeChan:  make(chan interface{}),
		retryCache: make(map[string]int),
		rulesMap:   make(map[string]*RetryRule),
	}

	if err := r.start(); err != nil {
		return nil, err
	}

	return r, nil
}

type retryer struct {
	conf       *options
	consumer   bus.Consumer
	producer   bus.Producer
	rules      []*RetryRule
	rulesMap   map[string]*RetryRule // key is the task type
	closeChan  chan interface{}
	retryCache map[string]int // holds retry counts
	sync.Mutex                // mutex for updating the retryCache
}

// start will:
// - load the retry rules
// - connect the consumer
// - connect the producer
// - begin listening for error tasks
func (r *retryer) start() error {
	if r.consumer == nil {
		return errors.New("unable to start - no consumer")
	}

	if r.producer == nil {
		return errors.New("unable to start - no producer")
	}

	// load rules into rules map
	r.loadRules()

	// TODO: ability to load retry state from a file
	// r.LoadRetries() // for now will log the retry state

	// start listening for error tasks
	r.listen()

	return nil
}

// loadRules will load all the retry rules into
// a local map for easier access.
func (r *retryer) loadRules() {
	for _, rule := range r.rules {
		key := rule.TaskType
		r.rulesMap[key] = rule
	}
}

// listen will start the listen loop to listen
// for failed tasks and then handle those failed
// tasks.
func (r *retryer) listen() {
	go r.doListen()
}

func (r *retryer) doListen() {
	for {
		// give the closeChan a change
		// to break the loop.
		select {
		case <-r.closeChan:
			return
		default:
		}

		// wait for a task
		msg, done, err := r.consumer.Msg()
		if err != nil {
			log.Println(err.Error())
			if done {
				return
			}
			continue
		}

		// attempt to create task
		var tsk *task.Task
		if len(msg) > 0 {
			tsk, err = task.NewFromBytes(msg)
			if err != nil {
				log.Println(err.Error())
				if done {
					return
				}
				continue
			}

			// evaluate task
			r.applyRule(tsk)
		}

		if done {
			return
		}
	}
}

// applyRule will
// - discover if the task is an error
// - look for a retry rule to apply
// - if the task needs to be retried then it is returned
// - if the task does not need to be retried the nil is returned
func (r *retryer) applyRule(tsk *task.Task) {
	rule, ok := r.rulesMap[tsk.Type]
	if !ok {
		return
	}

	key := makeCacheKey(tsk)
	r.Lock()
	defer r.Unlock()
	cnt, _ := r.retryCache[key]

	if tsk.Result == task.ErrResult {
		if cnt < rule.Retries {
			r.retryCache[key] = cnt + 1
			go r.doRetry(tsk, rule)
		} else {
			// produce to 'retry-failed' topic (optional but 'retry-failed' by default)
			if r.conf.RetryFailedTopic != "-" {
				err := r.producer.Send(r.conf.RetryFailedTopic, tsk.JSONBytes())
				if err != nil {
					log.Println(err.Error())
				}
			}
		}
	} else if cnt > 0 {
		// retry successful - now remove the cache
		delete(r.retryCache, key)
	}
}

// doRetry will wait (if requested by the rule)
// and then send the task to the outgoing channel
func (r *retryer) doRetry(tsk *task.Task, rule *RetryRule) {
	// will also add some built in jitter based on a percent of the wait time.
	time.Sleep(rule.Wait.Duration + jitterPercent(rule.Wait.Duration, 20))

	// create a new task just like the old one
	// and send it out.
	nTsk := task.New(tsk.Type, tsk.Info)

	topic := rule.TaskType
	if rule.Topic != "" {
		topic = rule.Topic
	}

	// produce to task topic
	err := r.producer.Send(topic, nTsk.JSONBytes())
	if err != nil {
		log.Println(err.Error())
	}

	// produce to 'retried' topic (optional but 'retried' by default
	if r.conf.RetriedTopic != "-" {
		err = r.producer.Send(r.conf.RetriedTopic, nTsk.JSONBytes())
		if err != nil {
			log.Println(err.Error())
		}
	}
}

// genJitter will return a time.Duration representing extra
// 'jitter' to be added to the wait time. Jitter is important
// in retry events since the original cause of failure can be
// due to too many jobs being processed at a time.
//
// By adding some jitter the retry events won't all happen
// at once but will get staggered to prevent the problem
// from happening again.
//
// 'p' is a percentage of the wait time. Duration returned
// is a random duration between 0 and p. 'p' should be a value
// between 0-100.
func jitterPercent(wait time.Duration, p int64) time.Duration {
	// p == 40
	maxJitter := (int64(wait) * p) / 100

	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Int63n(maxJitter))
}

func (r *retryer) close() error {
	// send close signal
	close(r.closeChan)

	// close the consumer
	if err := r.consumer.Stop(); err != nil {
		return err
	}

	// close the producer
	if err := r.producer.Stop(); err != nil {
		return err
	}

	return nil
}

// makeCacheKey will make a key string of the format:
// "task.Type" + "task.Info"
func makeCacheKey(tsk *task.Task) string {
	return tsk.Type + tsk.Info
}
