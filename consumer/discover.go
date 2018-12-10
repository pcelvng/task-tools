package consumer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
)

type discover struct {
	newConsumer func(topic string, c *nsq.Consumer)
	channel     string
	pollPeriod  time.Duration
	lookupds    []string
}

func (d *discover) run() {
	prevTopics := make([]string, 0)
	for ; ; time.Sleep(d.pollPeriod) {
		t, err := d.registerConsumer(prevTopics)
		if err != nil {
			log.Println("error:", err)
			continue
		}
		prevTopics = t
	}
}

// registerConsumer queries the nsqlookupds for all known topics and creates new consumer
// for topics not found in prevTopics
func (d *discover) registerConsumer(prevTopics []string) ([]string, error) {
	topicMap := make(map[string]struct{})
	for _, v := range prevTopics {
		topicMap[v] = struct{}{}
	}

	for _, host := range d.lookupds {
		topics, err := getTopics(host)
		if err != nil {
			return prevTopics, errors.Wrapf(err, "topic err %s", err)
		}

		for _, topic := range topics {
			if _, found := topicMap[topic]; !found {
				c, err := nsq.NewConsumer(topic, d.channel, nsq.NewConfig())
				if err != nil {
					return prevTopics, errors.Wrap(err, "consumer init err")
				}
				d.newConsumer(topic, c)
				topicMap[topic] = struct{}{}
			}
		}
	}
	topics := make([]string, 0)
	for t := range topicMap {
		topics = append(topics, t)
	}
	return topics, nil
}

// DiscoverTopics queries lookupds for any new topics, creates a consumer
// and calls the newConsumer function
func DiscoverTopics(newConsumer func(topic string, c *nsq.Consumer), channel string, pollperiod time.Duration, lookupds []string) {
	go (&discover{
		newConsumer: newConsumer,
		channel:     channel,
		pollPeriod:  pollperiod,
		lookupds:    lookupds,
	}).run()
}

func getTopics(host string) ([]string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%v/topics", host))
	if err != nil {
		return nil, err
	}
	body, _ := ioutil.ReadAll(resp.Body)
	r := &topicResponse{}
	err = json.Unmarshal(body, r)
	if len(r.Topics) != 0 {
		return r.Topics, err
	}
	return r.Data.Topics, err
}

type topicResponse struct {
	Topics []string `json:"topics"`
	Data   struct {
		Topics []string `json:"topics"`
	} `json:"data"`
}
