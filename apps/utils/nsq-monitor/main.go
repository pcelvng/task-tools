package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/hydronica/go-config"
	tools "github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/slack"
)

const description = `monitor nsq topic depth, and notify on build up of messages`

type AppConfig struct {
	slack.Slack `toml:"slack" comment:"slack notification options"`

	//Bus          string                 `toml:"bus" comment:"the bus type ie:nsq, kafka"`
	LookupdHost  string        `toml:"lookupd_hosts" comment:"host names of nsq lookupd servers"`
	DefaultLimit Limit         `toml:"default_limit" comment:"default topic limit, used when no topic is specified in the config"`
	Topics       []Limit       `toml:"topics" comment:"topics limits, this overrides the default limit"`
	PollPeriod   time.Duration `toml:"poll_period" comment:"the time between refresh on the topic list default is '5m'"`
	Port         int           `toml:"port" comment:"HTTP server port (0 = disabled)"`

	depthRegistry DepthRegistry       // a map of all topics
	nsqdNodes     map[string]struct{} // a map of nodes (producers)
	startTime     time.Time           // track when the service started
}

type Limit struct {
	Name  string  `toml:"name" comment:"topic name to monitor, use \"all\" for a catch all use case"`
	Depth int     `toml:"depth" comment:"the depth when to start receiving alerts"`
	Rate  float64 `toml:"rate" comment:"the rate of messages per second to alert on"`
}

func main() {
	app := &AppConfig{
		LookupdHost:  "127.0.0.1:4161",
		PollPeriod:   5 * time.Minute,
		DefaultLimit: Limit{Depth: 500, Rate: 3, Name: "all"},
		Topics:       []Limit{},
		Slack: slack.Slack{
			Prefix: "nsq-monitor",
		},
		startTime: time.Now(),
	}

	config.New(app).Version(tools.Version).Description(description).LoadOrDie()

	ctx, cancel := context.WithCancel(context.Background())
	monitorDone := make(chan struct{})
	httpDone := make(chan struct{})

	// Start the monitoring goroutine
	go func(ctx context.Context) {
		app.Monitor(ctx)
		monitorDone <- struct{}{}
	}(ctx)

	// Start the HTTP server if port is configured
	var httpServer *http.Server
	if app.Port > 0 {
		router := chi.NewRouter()
		router.Get("/info", app.infoHandler)

		httpServer = &http.Server{
			Addr:    fmt.Sprintf(":%d", app.Port),
			Handler: router,
		}

		go func() {
			log.Printf("HTTP server starting on port %d", app.Port)
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("HTTP server error: %v", err)
			}
			httpDone <- struct{}{}
		}()
	} else {
		log.Println("HTTP server disabled (port = 0)")
		close(httpDone) // Close immediately since no HTTP server is running
	}

	sigChan := make(chan os.Signal) // App signal handling
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Fatal("auto shutdown")
	case <-sigChan:
		fmt.Println("signal received, shutting down...")
		cancel()

		// Gracefully shutdown HTTP server if it's running
		if httpServer != nil {
			log.Println("shutting down HTTP server...")
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			if err := httpServer.Shutdown(shutdownCtx); err != nil {
				log.Printf("HTTP server forced shutdown: %v", err)
			}
		}

		<-monitorDone // wait for the monitor to finish
		<-httpDone    // wait for the HTTP server to finish
	}
}

func (a *AppConfig) Validate() (err error) {
	return nil
}

type AlertInfo struct {
	ActiveCount   int        `json:"active_count"`
	LastAlertTime *time.Time `json:"last_alert_time"`
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

	// Build grouped topics info
	topicGroups := make(map[string]TopicGroup)
	var activeAlerts int
	var lastAlert *time.Time

	for _, metric := range a.depthRegistry {
		rate := metric.Derivative() / a.PollPeriod.Seconds()
		depth := metric.Depth()

		// Find matching topic config in slice, or use default
		config := a.DefaultLimit // Start with default
		for _, topicLimit := range a.Topics {
			if topicLimit.Name == metric.Topic {
				config = topicLimit
				break
			}
		}

		// Determine channel status
		channelStatus := "ok"
		if metric.DepthAlert && metric.RateAlert {
			channelStatus = "both_exceeded"
		} else if metric.DepthAlert {
			channelStatus = "depth_exceeded"
		} else if metric.RateAlert {
			channelStatus = "rate_exceeded"
		}

		// Create channel info
		channelInfo := ChannelInfo{
			Channel:     metric.Channel,
			Depth:       depth,
			Rate:        rate,
			LastUpdated: metric.Last[D3].TimeStamp,
			Status:      channelStatus,
		}

		// Get or create topic group
		topicGroup, exists := topicGroups[metric.Topic]
		if !exists {
			topicGroup = TopicGroup{
				Status:     "ok",
				DepthLimit: config.Depth,
				RateLimit:  config.Rate,
				Channels:   []ChannelInfo{},
			}
		}

		// Add channel to topic group
		topicGroup.Channels = append(topicGroup.Channels, channelInfo)

		// Update topic-level status if any channel is alerted
		if channelStatus != "ok" && topicGroup.Status == "ok" {
			topicGroup.Status = "alerted"
		}

		// Store updated topic group
		topicGroups[metric.Topic] = topicGroup

		// Count active alerts (either depth or rate)
		if metric.DepthAlert || metric.RateAlert {
			activeAlerts++
			// For now, we'll use the last updated time as a proxy for alert time
			// This could be improved with proper alert timestamp tracking
			if lastAlert == nil || metric.Last[D3].TimeStamp.After(*lastAlert) {
				lastAlert = &metric.Last[D3].TimeStamp
			}
		}
	}

	// Sort channels within each topic group for consistent output
	for topicName, topicGroup := range topicGroups {
		slices.SortFunc(topicGroup.Channels, func(a, b ChannelInfo) int {
			return strings.Compare(a.Channel, b.Channel)
		})
		topicGroups[topicName] = topicGroup
	}

	response := InfoResponse{
		Service: ServiceInfo{
			Name:      "nsq-monitor",
			Version:   tools.Version,
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
