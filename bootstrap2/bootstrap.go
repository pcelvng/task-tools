package bootstrap2

import (
	"fmt"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

// genBusOptions will generate a helpful config options output
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

// genBusOptions will generate a helpful config options output
func genLauncherOptions(b *task.LauncherOptions) string {
	s := `# optional config for how launcher works. 
# max_in_progress is concurrent number of tasks allowed 
# lifetime_workers number of tasks to complete before terminating app 
# worker_kill_time how long the app waits before force stopping
[launcher]
`
	s += fmt.Sprintf("  done_topic=\"%v\"\n", b.DoneTopic)
	s += fmt.Sprintf("  %v=%v\n", "max_in_progress", b.MaxInProgress)

	return s
}
