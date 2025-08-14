package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
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

type TopicRegistry map[string]*TopicData
type ProducerMap map[string]map[string]*ChannelMetric

type TopicData struct {
	Status     string                    `json:"status"` // "ok" or "alerted"
	DepthLimit int                       `json:"depth_limit"`
	RateLimit  float64                   `json:"rate_limit"`
	Channels   map[string]*ChannelMetric `json:"channels"`
}

type ChannelMetric struct {
	Topic       string    `json:"topic"`
	Channel     string    `json:"channel"`
	Address     string    // the broadcast address
	Depth       int       `json:"depth"`
	Rate        float64   `json:"rate"`
	LastUpdated time.Time `json:"last_updated"`
	Status      string    `json:"status"` // "ok", "depth_exceeded", "rate_exceeded", or "both_exceeded"
	DepthAlert  bool      // Alert state for depth threshold
	RateAlert   bool      // Alert state for rate threshold
	Last        [3]Depth  // The last 3 depth values from 2 (very last) back up to 0
}

func (t ChannelMetric) Derivative() float64 {
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

// GetDepth return the last value
func (t ChannelMetric) GetDepth() int {
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
	app.topicRegistry = make(TopicRegistry)

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

	app.aggregateAndUpdateTopics(producerMap)
	app.CheckTopics()
	if msg := app.removeInactiveTopics(); len(msg) > 0 {
		app.Notify(msg, Warning)
	}
	return nil
}

// aggregateAndUpdateTopics processes producer data and updates the topic registry
// with current metrics and configurations
func (app *AppConfig) aggregateAndUpdateTopics(pm ProducerMap) {
	// Rotate historical depth values for all existing channels
	app.rotateDepthValues()

	// Iterate through each NSQ producer in the cluster
	for _, producerChannels := range pm {
		// Process each topic/channel combination from the current producer
		for channelKey, newChannel := range producerChannels {
			// Get or create topic in registry
			topicData, exists := app.topicRegistry[newChannel.Topic]
			if !exists {
				// Find matching topic config or use default
				config := app.DefaultLimit
				for _, topicLimit := range app.Topics {
					if topicLimit.Name == newChannel.Topic {
						config = topicLimit
						break
					}
				}

				topicData = &TopicData{
					Status:     "ok",
					DepthLimit: config.Depth,
					RateLimit:  config.Rate,
					Channels:   make(map[string]*ChannelMetric),
				}
				app.topicRegistry[newChannel.Topic] = topicData
			}

			// Get or create channel in topic
			existingChannel, channelExists := topicData.Channels[channelKey]
			if channelExists {
				// Add this producer's depth to the running total
				existingChannel.Last[D3].Value += newChannel.Last[D3].Value
				existingChannel.Last[D3].TimeStamp = time.Now()
			} else {
				// New channel - initialize with this producer's values
				topicData.Channels[channelKey] = newChannel
			}
		}
	}

	// Update derived values (depth, rate, status) for all channels
	app.updateChannelMetrics()
}

// rotateDepthValues rotates historical depth data for all channels
func (app *AppConfig) rotateDepthValues() {
	for _, topicData := range app.topicRegistry {
		for _, channel := range topicData.Channels {
			channel.Last[D1] = channel.Last[D2]
			channel.Last[D2] = channel.Last[D3]
			channel.Last[D3] = Depth{Value: 0, TimeStamp: time.Now()}
		}
	}
}

// updateChannelMetrics calculates current depth, rate, and status for all channels
func (app *AppConfig) updateChannelMetrics() {
	for _, topicData := range app.topicRegistry {
		for _, channel := range topicData.Channels {
			// Update depth and rate
			channel.Depth = channel.GetDepth()
			channel.Rate = channel.Derivative() / app.PollPeriod.Seconds()
			channel.LastUpdated = channel.Last[D3].TimeStamp

			// Update status based on current alert states
			if channel.DepthAlert && channel.RateAlert {
				channel.Status = "both_exceeded"
			} else if channel.DepthAlert {
				channel.Status = "depth_exceeded"
			} else if channel.RateAlert {
				channel.Status = "rate_exceeded"
			} else {
				channel.Status = "ok"
			}
		}
	}
}

// CheckTopics goes through each topic and alerts if the depth is above the threshold
// or if the derivative (rate of change) is above the threshold.
func (a *AppConfig) CheckTopics() {
	var alerts, cleared []string

	// Loop through all topics and their channels
	for _, topicData := range a.topicRegistry {
		topicHasAlert := false

		for _, channel := range topicData.Channels {
			// Determine current alert states using topic's configured limits
			currentDepthAlert := topicData.DepthLimit > 0 && channel.Depth > topicData.DepthLimit
			currentRateAlert := topicData.RateLimit > 0 && channel.Rate > topicData.RateLimit

			// Check for rate alert changes
			if channel.RateAlert && !currentRateAlert {
				// Rate alert cleared
				cleared = append(cleared, fmt.Sprintf("[%s/%s] rate cleared: %.2f <= %.2f", channel.Topic, channel.Channel, channel.Rate, topicData.RateLimit))
			} else if !channel.RateAlert && currentRateAlert {
				// New rate alert
				alerts = append(alerts, fmt.Sprintf("[%s/%s] rate exceeded: %.2f > %.2f", channel.Topic, channel.Channel, channel.Rate, topicData.RateLimit))
			}

			// Check for depth alert changes
			if channel.DepthAlert && !currentDepthAlert {
				// Depth alert cleared
				cleared = append(cleared, fmt.Sprintf("[%s/%s] depth cleared: %d <= %d", channel.Topic, channel.Channel, channel.Depth, topicData.DepthLimit))
			} else if !channel.DepthAlert && currentDepthAlert {
				// New depth alert
				alerts = append(alerts, fmt.Sprintf("[%s/%s] depth exceeded: %d > %d", channel.Topic, channel.Channel, channel.Depth, topicData.DepthLimit))
			}

			// Update alert states
			channel.DepthAlert = currentDepthAlert
			channel.RateAlert = currentRateAlert

			// Update channel status
			if channel.DepthAlert && channel.RateAlert {
				channel.Status = "both_exceeded"
			} else if channel.DepthAlert {
				channel.Status = "depth_exceeded"
			} else if channel.RateAlert {
				channel.Status = "rate_exceeded"
			} else {
				channel.Status = "ok"
			}

			// Track if this topic has any alerts
			if channel.DepthAlert || channel.RateAlert {
				topicHasAlert = true
				log.Printf("[%s/%s] depth: %d rate:%.2f/sec (ALERTED)", channel.Topic, channel.Channel, channel.Depth, channel.Rate)
			}
		}

		// Update topic-level status
		if topicHasAlert {
			topicData.Status = "alerted"
		} else {
			topicData.Status = "ok"
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

// removeInactiveTopics removes topics/channels that haven't been updated
func (a *AppConfig) removeInactiveTopics() string {
	for topicName, topicData := range a.topicRegistry {
		for channelKey, channel := range topicData.Channels {
			if channel.Last[D3].TimeStamp.IsZero() {
				delete(topicData.Channels, channelKey)
				// If topic has no channels left, remove the topic
				if len(topicData.Channels) == 0 {
					delete(a.topicRegistry, topicName)
				}
				return fmt.Sprintf("topic/channel not updated, removing: [%s] - {%s/%s} %s",
					channelKey, channel.Topic, channel.Channel, channel.Address)
			}
		}
	}
	return ""
}

// getNodes will Unmarshal the lookup nodes from the request body
func getNodes(host string) (producers []Producer, err error) {
	producers = make([]Producer, 0)
	body, err := makeRequest(fmt.Sprintf("http://%v/nodes?format=json", host))
	if err != nil {
		err = fmt.Errorf("makerequest error: %v", err)
		return nil, err
	}

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
		return nil, fmt.Errorf("err reading lookupd resp: %w", err)
	}

	return body, err
}

// InfoResponse represents the JSON response for the /info endpoint
type InfoResponse struct {
	Service    ServiceInfo           `json:"service"`
	NSQCluster NSQClusterInfo        `json:"nsq_cluster"`
	Topics     map[string]TopicGroup `json:"topics"`
	Alerts     AlertInfo             `json:"alerts"`
}

type ServiceInfo struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Uptime    string `json:"uptime"`
	StartedAt string `json:"started_at"`
}

type NSQClusterInfo struct {
	LookupdHost    string   `json:"lookupd_host"`
	ConnectedNodes []string `json:"connected_nodes"`
	PollPeriod     string   `json:"poll_period"`
}

type ConfigInfo struct {
	DefaultLimit Limit   `json:"default_limit"`
	TopicLimits  []Limit `json:"topic_limits"`
}

type TopicGroup struct {
	Status     string        `json:"status"` // "ok" or "alerted"
	DepthLimit int           `json:"depth_limit"`
	RateLimit  float64       `json:"rate_limit"`
	Channels   []ChannelInfo `json:"channels"`
}

type ChannelInfo struct {
	Channel     string    `json:"channel"`
	Depth       int       `json:"depth"`
	Rate        float64   `json:"rate"`
	LastUpdated time.Time `json:"last_updated"`
	Status      string    `json:"status"` // "ok", "depth_exceeded", "rate_exceeded", or "both_exceeded"
}

type AlertInfo struct {
	ActiveCount   int        `json:"active_count"`
	LastAlertTime *time.Time `json:"last_alert_time"`
}

// SetupRouter creates and configures the chi router
func (a *AppConfig) SetupRouter() *chi.Mux {
	r := chi.NewRouter()

	// Add some basic middleware
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)

	// Add the /info endpoint
	r.Get("/info", a.infoHandler)

	return r
}

// infoHandler handles the /info endpoint
func (a *AppConfig) infoHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Calculate uptime
	uptime := time.Since(a.startTime)

	// Get connected nodes
	var connectedNodes []string
	for node := range a.nsqdNodes {
		connectedNodes = append(connectedNodes, node)
	}
	slices.Sort(connectedNodes)

	// Build grouped topics info directly from the unified structure
	topicGroups := make(map[string]TopicGroup)
	var activeAlerts int
	var lastAlert *time.Time

	for topicName, topicData := range a.topicRegistry {
		var channels []ChannelInfo

		for _, channel := range topicData.Channels {
			channels = append(channels, ChannelInfo{
				Channel:     channel.Channel,
				Depth:       channel.Depth,
				Rate:        channel.Rate,
				LastUpdated: channel.LastUpdated,
				Status:      channel.Status,
			})

			// Count active alerts
			if channel.DepthAlert || channel.RateAlert {
				activeAlerts++
				if lastAlert == nil || channel.LastUpdated.After(*lastAlert) {
					lastAlert = &channel.LastUpdated
				}
			}
		}

		// Sort channels for consistent output
		slices.SortFunc(channels, func(a, b ChannelInfo) int {
			return strings.Compare(a.Channel, b.Channel)
		})

		topicGroups[topicName] = TopicGroup{
			Status:     topicData.Status,
			DepthLimit: topicData.DepthLimit,
			RateLimit:  topicData.RateLimit,
			Channels:   channels,
		}
	}

	response := InfoResponse{
		Service: ServiceInfo{
			Name:      "nsq-monitor",
			Version:   "1.0.0", // Could be imported from version package
			Uptime:    uptime.String(),
			StartedAt: a.startTime.Format(time.RFC3339),
		},
		NSQCluster: NSQClusterInfo{
			LookupdHost:    a.LookupdHost,
			ConnectedNodes: connectedNodes,
			PollPeriod:     a.PollPeriod.String(),
		},
		Topics: topicGroups,
		Alerts: AlertInfo{
			ActiveCount:   activeAlerts,
			LastAlertTime: lastAlert,
		},
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		log.Printf("Error encoding /info response: %v", err)
		return
	}
}
