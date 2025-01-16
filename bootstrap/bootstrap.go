package bootstrap

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"

	"github.com/pcelvng/task-tools/file"
)

var sigChan = make(chan os.Signal, 1) // signal handling

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
	Username     string `toml:"username" commented:"true"`
	Password     string `toml:"password" commented:"true"`
	Host         string `toml:"host" comment:"host can be 'host:port', 'host', 'host:' or ':port'"`
	DBName       string `toml:"dbname"`
	Serializable bool   `toml:"serializable" comment:"set isolation level to serializable, required for proper writing to database" commented:"true"`
	SSLMode      string `toml:"sslmode" comment:"default is disable, use require for ssl"`
	SSLCert      string `toml:"sslcert"`
	SSLKey       string `toml:"sslkey"`
	SSLRootcert  string `toml:"sslrootcert"`
}

// newConsumer is a convenience method that will use
// the bus options information to create a new consumer
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

	consumer, err := bus.NewConsumer(&bOpt)
	if err != nil {
		log.Fatalf("NewConsumer Error: %v", err)
	}
	return consumer
}

// newProducer will use the bus options information
// to create a new producer instance. Note that bOpt is
// now a copy of the original options since it's not a pointer.
func newProducer(bOpt bus.Options) bus.Producer {
	producer, err := bus.NewProducer(&bOpt)
	if err != nil {
		log.Fatalf("NewProducer error: %v", err)
	}
	return producer
}

// Duration is a wrapper around time.Duration
// and allows for automatic toml string parsing of
// time.Duration values. Use this type in a
// custom options for automatic serializing and
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

// genBusOptions will generate a helpful options options output
func genBusOptions(b *bus.Options) string {
	s := `# task message bus (nsq, pubsub, file, stdio)
# if in_bus and out_bus are blank they will default to the main bus. 
[bus]
`
	s += fmt.Sprintf("  bus=\"%v\"\n", b.Bus)
	s += fmt.Sprintf("  #%v=\"%v\"\n", "in_topic", b.InTopic)
	s += fmt.Sprintf("  #%v=\"%v\"\n", "in_channel", b.InChannel)

	if b.Bus == "pubsub" {
		s += fmt.Sprintf("  #%v=\"%v\"\n", "pubsub_host", "emulator host")
		s += fmt.Sprintf("  #%v=\"%v\"\n", "pubsub_id", b.ProjectID)
		s += fmt.Sprintf("  #%v=\"%v\"\n", "json_auth", b.JSONAuth)
	}
	if b.Bus == "nsq" {
		s += fmt.Sprintf("  #%v=%v\n", "lookupd_hosts", b.LookupdHosts)
		s += fmt.Sprintf("  #%v=%v\n", "nsqd_hosts", b.NSQdHosts)
	}

	return s
}

// genBusOptions will generate a helpful options options output
func genLauncherOptions(b *task.LauncherOptions) string {
	s := `# optional options for how launcher works. 
# max_in_progress is concurrent number of tasks allowed 
# lifetime_workers number of tasks to complete before terminating app 
# worker_kill_time how long the app waits before force stopping
[launcher]
`
	s += fmt.Sprintf("  done_topic=\"%v\"\n", b.DoneTopic)
	s += fmt.Sprintf("  %v=%v\n", "max_in_progress", b.MaxInProgress)

	return s
}
