package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hydronica/go-config"
	"github.com/pcelvng/task-tools/slack"

	"github.com/rakuten-rad-insights/iap-pipeline/internal/version"
)

const description = `monitor nsq topic depth, and notify on build up of messages`

type AppConfig struct {
	slack.Slack `toml:"slack" comment:"slack notification options"`

	//Bus          string                 `toml:"bus" comment:"the bus type ie:nsq, kafka"`
	LookupdHost  string        `toml:"lookupd_hosts" comment:"host names of nsq lookupd servers"`
	DefaultLimit Limit         `toml:"default_limit" comment:"default topic limit, used when no topic is specified in the config"`
	Topics       []Limit       `toml:"topics" comment:"topics limits, this overrides the default limit"`
	PollPeriod   time.Duration `toml:"poll_period" comment:"the time between refresh on the topic list default is '5m'"`

	depthRegistry DepthRegistry       // a map of all topics
	nsqdNodes     map[string]struct{} // a map of nodes (producers)
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
	}

	config.New(app).Version(version.Version).Description(description).LoadOrDie()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func(ctx context.Context) {
		app.Monitor(ctx)
		done <- struct{}{}
	}(ctx)

	sigChan := make(chan os.Signal) // App signal handling
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Fatal("auto shutdown")
	case <-sigChan:
		fmt.Println("signal received, shutting down...")
		cancel()
		<-done // wait for the monitor to finish
	}
}

func (a *AppConfig) Validate() (err error) {
	return nil
}
