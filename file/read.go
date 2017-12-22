package file

import "github.com/pcelvng/task-tools/file/noop"

func NewStatsReader() (StatsReader, error) {
	return noop.NewReader(), nil
}