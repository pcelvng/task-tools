package retry

import (
	"time"

	"github.com/pcelvng/task/bus"
)

func NewOptions() *Options {
	return &Options{
		Options:          *bus.NewOptions(""),
		DoneTopic:        "done",
		DoneChannel:      "retry",
		RetriedTopic:     "retried",
		RetryFailedTopic: "retry-failed",
		RetryRules:       []*RetryRule{{TaskType: "task-type", Retries: 5, Wait: duration(500 * time.Millisecond)}},
	}
}

func (o *Options) Validate() error { return nil }

type Options struct {
	bus.Options `toml:"bus"`

	// topic and channel to listen to
	// done tasks for retry review.
	DoneTopic        string `toml:"done_topic" commented:"true"`
	DoneChannel      string `toml:"done_channel" commented:"true"`
	RetriedTopic     string `toml:"retried_topic" commented:"true" comment:"all retries published to this topic (disable with \"-\" value)"`
	RetryFailedTopic string `toml:"retry_failed_topic" commented:"true" comment:"all failures (retried and failed) published to this topic"`

	// retry rules
	RetryRules []*RetryRule `toml:"rule" commented:"true"`
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

func (d duration) MarshalTOML() ([]byte, error) {
	return []byte(d.Duration().String()), nil
}

func (d duration) Duration() time.Duration {
	return time.Duration(d)
}
