package file

import "github.com/pcelvng/task-tools/file/noop"

func NewStatsWriter(pth string, _ *FileConfig) (StatsWriter, error) {
	return noop.NewWriter(pth), nil
}
