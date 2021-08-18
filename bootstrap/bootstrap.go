package bootstrap

import (
	"flag"
	"os"
	"time"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
)

var (
	sigChan     = make(chan os.Signal, 1) // signal handling
	configPth   = flag.String("config", "", "application config toml file")
	c           = flag.String("c", "", "alias to -config")
	showVersion = flag.Bool("version", false, "show WorkerApp version and build info")
	ver         = flag.Bool("v", false, "alias to -version")
	genConfig   = flag.Bool("gen-config", false, "generate a config toml file to stdout")
	g           = flag.Bool("g", false, "alias to -gen-config")
)

// Validator provides a standard
// method for running underlying validation
// for underlying object values.
type Validator interface {
	Validate() error
}

// NilValidator satisfies the
// Validator interface but does
// nothing.
type NilValidator struct{}

func (v *NilValidator) Validate() error {
	return nil
}

// fileOptions are only included at the request of the user.
// If added they are made available with the application file
// options object which can be accessed from the WorkerApp object.
type fileOptions struct {
	FileOpt file.Options `toml:"file"`
}

// mysqlOptions are only added at the request of the user.
// If they are added then the bootstrap WorkerApp will automatically
// attempt to connect to mysql.
type mysqlOptions struct {
	MySQL DBOptions `toml:"mysql"`
}

// postgresOptions are only added at the request of the user.
// If they are added then the bootstrap WorkerApp will automatically
// attempt to connect to postgres.
type pgOptions struct {
	Postgres DBOptions `toml:"postgres"`
}

// general options for http-status health checks
type statsOptions struct {
	HttpPort int `toml:"status_port" comment:"http service port for request health status"`
}

type DBOptions struct {
	Username        string `toml:"username" commented:"true"`
	Password        string `toml:"password" commented:"true"`
	Host            string `toml:"host" comment:"host can be 'host:port', 'host', 'host:' or ':port'"`
	DBName          string `toml:"dbname"`
	Serializable    bool   `toml:"serializable" comment:"set isolation level to serializable, required for proper writing to database" commented:"true"`
	MaxConns        int    `toml:"max_conns"`
	MaxIdleConns    int    `toml:"max_idle_conns"`
	MaxConnLifeMins int    `toml:"max_life_min"`
}

// newConsumer is a convenience method that will use
// the bus config information to create a new consumer
// instance. Can optionally provide a topic and channel
// on which to consume. All other bus options are the same.
// Note that bOpt is a copy of the original options since it's
// not a pointer.
func newConsumer(bOpt bus.Options, topic, channel string) bus.Consumer {
	if topic != "" {
		bOpt.InTopic = topic
	}

	if channel != "" {
		bOpt.InChannel = channel
	}

	consumer, _ := bus.NewConsumer(&bOpt)
	return consumer
}

// newProducer will use the bus config information
// to create a new producer instance. Note that bOpt is
// now a copy of the original config since it's not a pointer.
func newProducer(bOpt bus.Options) bus.Producer {
	producer, _ := bus.NewProducer(&bOpt)
	return producer
}

// Duration is a wrapper around time.Duration
// and allows for automatic toml string parsing of
// time.Duration values. Use this type in a
// custom config for automatic serializing and
// de-serializing of time.Duration.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d *Duration) MarshalTOML() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}
