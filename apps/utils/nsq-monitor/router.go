package main

import (
	"encoding/json"
	"log"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

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
