package bootstrap

import (
	"context"

	"github.com/pcelvng/task"
)

type dummy struct{}

func (d *dummy) DoTask(ctx context.Context) (task.Result, string) {
	r := "dummy result"
	tr := task.Result(r)
	return tr, ""
}

func dummyWorker(_ string) task.Worker {
	var dumbWorker dummy
	return &dumbWorker
}
