package retry

import (
	"time"

	"github.com/pcelvng/task/bus"
)

func newOptions() *options {
	return &options{
		Options:          *bus.NewOptions(""),
		DoneTopic:        "done",
		DoneChannel:      "retry",
		RetriedTopic:     "retried",
		RetryFailedTopic: "retry-failed",
	}
}

type options struct {
	bus.Options

	// topic and channel to listen to
	// done tasks for retry review.
	DoneTopic        string `toml:"done_topic"`
	DoneChannel      string `toml:"done_channel"`
	RetriedTopic     string `toml:"retried_topic"`      // all retries published to this topic (disable with "-" value)
	RetryFailedTopic string `toml:"retry_failed_topic"` // all failures (retried and failed) published to this topic

	// retry rules
	RetryRules []*RetryRule `toml:"rule"`
}

type RetryRule struct {
	TaskType string   `toml:"type"`
	Retries  int      `toml:"retry"`
	Wait     duration `toml:"wait"`  // duration to wait before creating and sending new task
	Topic    string   `toml:"topic"` // topic override (default is TaskType value)
}

type duration time.Duration

func (d *duration) UnmarshalText(text []byte) error {
	dur, err := time.ParseDuration(string(text))
	*d = duration(dur)
	return err
}

func (d duration) Duration() time.Duration {
	return time.Duration(d)
}
