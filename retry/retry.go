package retry

import (
	"errors"
	"log"
	"math/rand"
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

type Retryer struct {
	conf       *Options
	consumer   bus.Consumer
	producer   bus.Producer
	rulesMap   map[string]*RetryRule // key is the task type
	closeChan  chan interface{}
	retryCache map[string]int // holds retry counts
	sync.Mutex                // mutex for updating the retryCache
	checkFunc  CheckFunc
}

func New(conf *Options) (*Retryer, error) {
	rand.Seed(time.Now().UnixNano())
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

	r := &Retryer{
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
func (r *Retryer) SetCheckFunc(fn CheckFunc) *Retryer {
	r.checkFunc = fn
	return r
}

// Start will:
// - load the retry rules
// - connect the consumer
// - connect the producer
// - begin listening for error tasks
func (r *Retryer) Start() error {
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
func (r *Retryer) listen() {
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
func (r *Retryer) applyRule(tsk *task.Task) {
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
func (r *Retryer) doRetry(tsk *task.Task, rule *RetryRule) {
	// will also add some built in jitter based on a percent of the wait time.
	d := rule.Wait.Duration() + jitterPercent(rule.Wait.Duration(), 20)
	time.Sleep(d)

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

func (r *Retryer) close() error {
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
	if wait == 0 {
		return 0
	}
	maxJitter := (int64(wait) * p) / 100

	return time.Duration(rand.Int63n(maxJitter))
}

// makeCacheKey will make a key string of the format:
// "task.Type" + "task.Info"
func makeCacheKey(tsk *task.Task) string {
	return tsk.Type + tsk.Info
}
