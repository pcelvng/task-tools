package bootstrap

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/bus/info"
	"github.com/stretchr/testify/assert"
)

type dummy struct{}

func TestInfoStats(t *testing.T) {
	createConfigFile("test.config")
	defer os.Remove("test.config")
	flag.Set("config", "test.config")

	nv := NilValidator{}
	app := NewWorkerApp("test", dummyWorker, &nv)
	app.wkrOpt.BusOpt = &bus.Options{Bus: "stdio"}
	app.consumer = app.NewConsumer("test_topic", "test_channel")
	app.producer = app.NewProducer()
	app.Initialize()

	i := app.InfoStats()

	e := Info{
		LauncherStats: task.LauncherStats{},
		ProducerStats: &info.Producer{Bus: "stdout", Sent: make(map[string]int)},
		ConsumerStats: &info.Consumer{Bus: "/dev/stdin"},
	}

	assert.Equal(t, e.ConsumerStats, i.ConsumerStats)
	assert.Equal(t, e.ProducerStats, i.ProducerStats)
	assert.NotEmpty(t, i.LauncherStats.RunTime)
}

func createConfigFile(filename string) {
	err := ioutil.WriteFile(filename,
		[]byte(`status_port = 11000

			[bus]
			
				# task message bus (nsq, file, stdio)
				bus = "stdio"
				in_bus = "stdin"
				out_bus = "stdout"

			[launcher]`), 0666)
	if err != nil {
		panic(filename + " file couldn't be written " + err.Error())
	}
}

func (d *dummy) DoTask(ctx context.Context) (task.Result, string) {
	r := "dummy result"
	tr := task.Result(r)
	return tr, ""
}

func dummyWorker(_ string) task.Worker {
	var dumbWorker dummy
	return &dumbWorker
}
