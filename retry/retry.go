package retry

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

// CheckFunc is a function that checks a task to see if it should be retried.
// this may be used to modify the task struct
type CheckFunc func(*task.Task) bool

func defaultCheck(tsk *task.Task) bool {
	return tsk.Result == task.ErrResult
}

type retryer struct {
	conf       *options
	consumer   bus.Consumer
	producer   bus.Producer
	rulesMap   map[string]*RetryRule // key is the task type
	closeChan  chan interface{}
	retryCache map[string]int // holds retry counts
	sync.Mutex                // mutex for updating the retryCache
	checkFunc  CheckFunc
}

func New(conf *options) (*retryer, error) {
	if len(conf.RetryRules) == 0 {
		return nil, errors.New("no retry rules specified")
	}

	// map over done topic and channel for consumer
	conf.Options.InTopic = conf.DoneTopic
	conf.Options.InChannel = conf.DoneChannel

	// make consumer
	c, err := bus.NewConsumer(&conf.Options)
	if err != nil {
		return nil, err
	}

	// make producer
	p, err := bus.NewProducer(&conf.Options)
	if err != nil {
		return nil, err
	}

	r := &retryer{
		conf:       conf,
		consumer:   c,
		producer:   p,
		closeChan:  make(chan interface{}),
		retryCache: make(map[string]int),
		rulesMap:   make(map[string]*RetryRule),
		checkFunc:  defaultCheck,
	}

	// load rules into rules map
	for _, rule := range conf.RetryRules {
		key := rule.TaskType
		r.rulesMap[key] = rule
	}

	// TODO: ability to load retry state from a file
	// r.LoadRetries() // for now will log the retry state

	return r, nil
}

// SetCheckFunc overrides the default checkFunc
func (r *retryer) SetCheckFunc(fn CheckFunc) *retryer {
	r.checkFunc = fn
	return r
}

// Start will:
// - load the retry rules
// - connect the consumer
// - connect the producer
// - begin listening for error tasks
func (r *retryer) Start() error {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// Start listening for error tasks
	go r.listen()

	select {
	case <-sigChan:
		log.Println("closing...")
		return r.close()
	}
}

// listen for and handle failed tasks.
func (r *retryer) listen() {
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

	retryTask := r.checkFunc(tsk)
	key := makeCacheKey(tsk)
	r.Lock()
	defer r.Unlock()
	cnt, _ := r.retryCache[key]
	if retryTask {
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
			delete(r.retryCache, key)
		}
	} else if cnt > 0 {
		// retry successful - now remove the cache
		delete(r.retryCache, key)
	}
}

// doRetry will wait (if requested by the rule)
// and then send the task to the outgoing channel
func (r *retryer) doRetry(tsk *task.Task, rule *RetryRule) {
	time.Sleep(rule.Wait.Duration())

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
