package file

import "github.com/pcelvng/task-tools/file/noop"

func NewStatsWriter() (StatsWriter, error) {
	return noop.NewWriter(), nil
}