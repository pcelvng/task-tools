package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/fatih/color"
	jsoniter "github.com/json-iterator/go"
	"github.com/pcelvng/task-tools/slack"
)

// notification status
const (
	OK int = iota
	Warning
	Critical
)

// Last 5 depths
const (
	D1 int = iota
	D2
	D3
)

type DepthRegistry map[string]DepthMetric
type ProducerMap map[string]DepthRegistry

type DepthMetric struct {
	Topic   string // The topic value
	Channel string // The channel value
	//	SendNotify bool     // Used to keep sending notifications, even if config criteria is not met
	Address string   // the broadcast address
	Alerted bool     //
	Last    [3]Depth // The last 3 depth values from 2 (very last) back up to 0
}

func (t DepthMetric) Derivative() float64 {
	v1, v2, v3 := t.Last[D1], t.Last[D2], t.Last[D3]

	if v1.TimeStamp.IsZero() || v2.TimeStamp.IsZero() || v3.TimeStamp.IsZero() {
		return 0
	}

	// Calculate time intervals
	h1 := v2.TimeStamp.Sub(v1.TimeStamp).Seconds()
	h2 := v3.TimeStamp.Sub(v2.TimeStamp).Seconds()

	// Check for equal spacing
	if h1 == 0 || h2 == 0 || v1.TimeStamp.Equal(v3.TimeStamp) {
		return 0
	}

	// If spacing is (roughly) equal, use central difference
	if math.Abs(h1-h2) < 1e-6 {
		return float64(v3.Value-v1.Value) / (v3.TimeStamp.Sub(v1.TimeStamp).Seconds())
	}

	// For unequal spacing, use general three-point formula
	// f'(x2) = ((x2-x1)^2 * (f3-f2)/(x3-x2) + (x3-x2)^2 * (f2-f1)/(x2-x1)) / ((x3-x1)*(x3-x2)+(x2-x1)*(x2-x3))
	num := (h2*h2)*(float64(v2.Value)-float64(v1.Value))/h1 + (h1*h1)*(float64(v3.Value)-float64(v2.Value))/h2
	denom := h1 + h2
	return num / denom
}

// Depth return the last value
func (t DepthMetric) Depth() int {
	return t.Last[D3].Value
}

type Depth struct {
	Value     int
	TimeStamp time.Time
}

func (app *AppConfig) Monitor(ctx context.Context) {
	fmt.Println("starting nsq Monitor")
	app.Slack.Notify("starting nsq monitor with poll period of"+app.PollPeriod.String(), slack.OK)

	app.nsqdNodes = make(map[string]struct{})
	app.depthRegistry = make(DepthRegistry)

	fmt.Println("configs loaded:")
	fmt.Printf("\tdefault - %+v\n", app.DefaultLimit)
	for _, c := range app.Topics {
		fmt.Printf("\t%s - %+v\n", c.Name, c)
	}
	ticker := time.NewTicker(app.PollPeriod)
	defer ticker.Stop()
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}

	// Application Loop
	for {
		select {
		case <-ctx.Done():
			log.Println("context done, exiting monitor loop")
			return
		case <-ticker.C:
			if err := app.Run(); err != nil {
				log.Fatal(err)
			}
		} // app loop
	}
}

func (app *AppConfig) Run() error {
	log.Println("running poll check")

	producers, err := getNodes(app.LookupdHost)
	if err != nil {
		return fmt.Errorf("error querying for lookupd nodes: %w", err)
	}

	nsqdProducers := make(map[string]struct{})
	producerMap := make(ProducerMap)
	for _, p := range producers {
		if p.stats.Version == "" {
			return fmt.Errorf("stats data error, closing ")
		}

		if p.stats.Health != "OK" {
			//app.Prefix = "NSQ Health"
			msg := fmt.Sprintf("nsqd node health not OK - %s - %s:%d",
				p.stats.Health, p.BroadcastAddress, p.HTTPPort)
			app.Notify(msg, Critical)
			return errors.New(msg)
		}
		// set all the current nsqdProducers
		nsqdProducers[p.BroadcastAddress] = struct{}{}
		// set the latest topics values for current producer
		producerMap[p.BroadcastAddress] = p.getTopics()

	} // producer check loop

	// nsqd node connection changes
	if len(app.nsqdNodes) == 0 {
		var nodes string
		if len(nsqdProducers) == 0 {
			nodes = "no nodes found"
		}
		for k := range nsqdProducers {
			app.nsqdNodes[k] = struct{}{}
			nodes += "\t(" + k + ")"
		}
		log.Printf("nsqd nodes connected: %v", nodes)
	}

	for _, p := range producers {
		if _, ok := app.nsqdNodes[p.BroadcastAddress]; !ok {
			msg := fmt.Sprintf("nsqd at '%v' is now attached to lookupd at '%v'",
				p.BroadcastAddress, app.LookupdHost)
			app.Notify(msg, OK)
			app.nsqdNodes[p.BroadcastAddress] = struct{}{}
		}
	}

	for b := range app.nsqdNodes {
		if _, ok := nsqdProducers[b]; !ok {
			msg := fmt.Sprintf("nsqd at '%v' is no longer attached to lookupd at '%v'",
				b, app.LookupdHost)
			app.Notify(msg, Warning)
			delete(app.nsqdNodes, b)
		}
	}

	app.depthRegistry.AggDepths(producerMap)
	app.CheckTopics()
	if msg := removeTopics(app.depthRegistry); len(msg) > 0 {
		app.Notify(msg, Warning)
	}
	return nil
}

// AggDepths aggregates NSQ topic depth values across all producers in the cluster.
// It rotates historical depth data and sums current depth values from each producer
// to calculate the total queue depth for each topic/channel combination.
func (tm DepthRegistry) AggDepths(pm ProducerMap) {
	// Rotate historical depth values to make room for new measurements
	// This shifts D1 ← D2 ← D3 ← (new value), preserving trend data for derivative calculations
	rotateDepth(tm)

	// Iterate through each NSQ producer in the cluster
	for _, pv := range pm {
		// Process each topic/channel combination from the current producer
		for tk, tc := range pv {
			// Check if this topic/channel already exists in our aggregated registry
			atv, ok := tm[tk]
			// If exists, add this producer's depth to the running total
			if ok {
				atv.Last[D3].Value += tc.Last[D3].Value
				atv.Last[D3].TimeStamp = time.Now()
				// Update the aggregated value
				tm[tk] = atv
			} else {
				// New topic/channel - initialize with this producer's values
				tm[tk] = pv[tk]
			}
		}
	}

}

// CheckTopics goes through each topic and alerts if the depth is above the threshold
// or if the derivative (rate of change) is above the threshold.
func (a *AppConfig) CheckTopics() {
	// loop though all topics and channels, check against config
	var alerts, cleared []string
	for _, v := range a.depthRegistry {
		rate := v.Derivative() / a.PollPeriod.Seconds()
		depth := v.Last[D3].Value

		// Find matching topic config in slice, or use default
		config := a.DefaultLimit // Start with default
		for _, topicLimit := range a.Topics {
			if topicLimit.Name == v.Topic {
				config = topicLimit
				break
			}
		}

		if v.Alerted {
			if rate < config.Rate {
				v.Alerted = false
				cleared = append(cleared, fmt.Sprintf("[%s/%s] rate exceeded: %.2f > %.2f", v.Topic, v.Channel, rate, config.Rate))
			}
			if depth < config.Depth {
				v.Alerted = false
				cleared = append(cleared, fmt.Sprintf("[%s/%s] depth exceeded: %d > %d", v.Topic, v.Channel, depth, config.Depth))
			}
			log.Printf(color.GreenString("[%s/%s] depth: %d rate:%.2f/sec"), v.Topic, v.Channel, depth, rate)
			continue
		}
		if config.Rate > 0 && rate > config.Rate {
			v.Alerted = true
			alerts = append(alerts, fmt.Sprintf("[%s/%s] rate exceeded: %.2f > %.2f", v.Topic, v.Channel, rate, config.Rate))
		}
		if config.Depth > 0 && depth > config.Depth {
			v.Alerted = true
			alerts = append(alerts, fmt.Sprintf("[%s/%s] depth exceeded: %d > %d", v.Topic, v.Channel, depth, config.Depth))
		}
		if v.Alerted {
			log.Printf(color.RedString("[%s/%s] depth: %d rate:%.2f/sec"), v.Topic, v.Channel, depth, rate)
		} else if depth > 0 || rate > 0 {
			log.Printf("[%s/%s] depth: %d rate:%.2f/sec", v.Topic, v.Channel, depth, rate)
		}

	}
	slices.Sort(alerts)
	slices.Sort(cleared)
	if len(alerts) > 0 {
		a.Notify(strings.Join(alerts, "\n"), slack.Critical)
	}
	if len(cleared) > 0 {
		a.Notify(strings.Join(cleared, "\n"), slack.OK)
	}
}

// getNodes will Unmarshal the lookup nodes from the request body
func getNodes(host string) (producers []Producer, err error) {
	producers = make([]Producer, 0)
	body, err := makeRequest(fmt.Sprintf("http://%v/nodes?format=json", host))
	if err != nil {
		err = fmt.Errorf("makerequest error: %v", err)
		return nil, err
	}

	json := jsoniter.ConfigFastest
	var lookupNodes LookupNodes
	err = json.Unmarshal(body, &lookupNodes)
	if err != nil {
		err = fmt.Errorf("json unmarshal error: %v", err)
		return nil, err
	}

	// check to make sure there are producers returned
	if len(lookupNodes.Producers) == 0 {
		return producers, errors.New("no producers found in body")
	}
	// Loop over the producers to get the brodcast address of each node
	for _, p := range lookupNodes.Producers {
		if strings.Contains(host, "localhost") || strings.Contains(host, "127.0.0.1") {
			p.BroadcastAddress = "localhost"
		}
		if p.BroadcastAddress != "" && p.HTTPPort != 0 {
			address := fmt.Sprintf("%s:%d", p.BroadcastAddress, p.HTTPPort)
			stats, err := getStats(address)
			if err != nil {
				err = fmt.Errorf("getstats error: %v", err)
				return producers, err
			}
			p.stats = stats
		}
		producers = append(producers, p)
	}

	return producers, err
}

func getStats(host string) (stats NsqdStats, err error) {
	request := fmt.Sprintf("http://%v/stats?format=json", host)
	body, err := makeRequest(request)
	if err != nil {
		err = fmt.Errorf("makerequest error: %v", err)
		return stats, err
	}

	json := jsoniter.ConfigFastest
	err = json.Unmarshal(body, &stats)
	if err != nil {
		err = fmt.Errorf("json unmarshal error: %v", err)
		return stats, err
	}

	return stats, err
}

func makeRequest(request string) ([]byte, error) {
	resp, err := http.Get(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// make sure 200 ok
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("http status code: %v", resp.StatusCode)
	}

	// read resp Body
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("err reading lookupd resp: %v", err))
	}

	return body, err
}
