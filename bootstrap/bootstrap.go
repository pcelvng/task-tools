package bootstrap

import (
	"fmt"
	"log"
	"os"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
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

// genBusOptions will generate a helpful options output
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

// genBusOptions will generate a helpful options output
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
