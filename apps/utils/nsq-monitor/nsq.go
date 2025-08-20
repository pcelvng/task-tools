package main

import (
	"time"
)

// map["BroadcastAddress:HTTPPort"]

// represents the last 5 depth values from 0 to 4

type LookupNodes struct {
	Producers []Producer `json:"producers"`
}

type Producer struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`

	stats NsqdStats
}

type NsqdStats struct {
	Version   string  `json:"version"`
	Health    string  `json:"health"`
	StartTime int     `json:"start_time"`
	Topics    []Topic `json:"topics"`
	Memory    struct {
		HeapObjects       int `json:"heap_objects"`
		HeapIdleBytes     int `json:"heap_idle_bytes"`
		HeapInUseBytes    int `json:"heap_in_use_bytes"`
		HeapReleasedBytes int `json:"heap_released_bytes"`
		GcPauseUsec100    int `json:"gc_pause_usec_100"`
		GcPauseUsec99     int `json:"gc_pause_usec_99"`
		GcPauseUsec95     int `json:"gc_pause_usec_95"`
		NextGcBytes       int `json:"next_gc_bytes"`
		GcTotalRuns       int `json:"gc_total_runs"`
	} `json:"memory"`
}

type Channel struct {
	ChannelName          string   `json:"channel_name"`
	Depth                int      `json:"depth"`
	BackendDepth         int      `json:"backend_depth"`
	InFlightCount        int      `json:"in_flight_count"`
	DeferredCount        int      `json:"deferred_count"`
	MessageCount         int      `json:"message_count"`
	RequeueCount         int      `json:"requeue_count"`
	TimeoutCount         int      `json:"timeout_count"`
	Clients              []Client `json:"clients"`
	Paused               bool     `json:"paused"`
	E2EProcessingLatency struct {
		Count       int         `json:"count"`
		Percentiles interface{} `json:"percentiles"`
	} `json:"e2e_processing_latency"`
}

type Client struct {
	ClientID                      string `json:"client_id"`
	Hostname                      string `json:"hostname"`
	Version                       string `json:"version"`
	RemoteAddress                 string `json:"remote_address"`
	State                         int    `json:"state"`
	ReadyCount                    int    `json:"ready_count"`
	InFlightCount                 int    `json:"in_flight_count"`
	MessageCount                  int    `json:"message_count"`
	FinishCount                   int    `json:"finish_count"`
	RequeueCount                  int    `json:"requeue_count"`
	ConnectTs                     int    `json:"connect_ts"`
	SampleRate                    int    `json:"sample_rate"`
	Deflate                       bool   `json:"deflate"`
	Snappy                        bool   `json:"snappy"`
	UserAgent                     string `json:"user_agent"`
	TLS                           bool   `json:"tls"`
	TLSCipherSuite                string `json:"tls_cipher_suite"`
	TLSVersion                    string `json:"tls_version"`
	TLSNegotiatedProtocol         string `json:"tls_negotiated_protocol"`
	TLSNegotiatedProtocolIsMutual bool   `json:"tls_negotiated_protocol_is_mutual"`
}

type Topic struct {
	TopicName            string    `json:"topic_name"`
	Channels             []Channel `json:"channels"`
	Depth                int       `json:"depth"`
	BackendDepth         int       `json:"backend_depth"`
	MessageCount         int       `json:"message_count"`
	Paused               bool      `json:"paused"`
	E2EProcessingLatency struct {
		Count       int         `json:"count"`
		Percentiles interface{} `json:"percentiles"`
	} `json:"e2e_processing_latency"`
}

// removes any topics that have not been updated

// creates a map of all current topics and the depth values
// for the current Producer
func (p Producer) getTopics() (channels map[string]*ChannelMetric) {
	// set all new topic check values from request
	channels = make(map[string]*ChannelMetric)
	for _, t := range p.stats.Topics {
		// there are no channels for the given topic
		if len(t.Channels) == 0 {
			tc := &ChannelMetric{
				Topic:   t.TopicName,
				Channel: "(none)",
				Address: p.BroadcastAddress,
			}

			tc.Last[D3] = Depth{Value: t.Depth, TimeStamp: time.Now()}
			channels[tc.Topic+"/"+tc.Channel] = tc
		}

		// loop though all channels
		for _, c := range t.Channels {
			tc := &ChannelMetric{
				Topic:   t.TopicName,
				Channel: c.ChannelName,
				Address: p.BroadcastAddress,
			}

			tc.Last[D3] = Depth{Value: c.Depth, TimeStamp: time.Now()}
			channels[tc.Topic+"/"+tc.Channel] = tc
		}
	}
	return channels
}
