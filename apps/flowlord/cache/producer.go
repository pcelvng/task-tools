package cache

import (
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

// SendFunc extends the given producers send function by adding any task sent to the cache.
func (m *Memory) SendFunc(p bus.Producer) func(string, *task.Task) error {
	return func(topic string, tsk *task.Task) error {
		m.Add(*tsk)
		return p.Send(topic, tsk.JSONBytes())
	}
}
