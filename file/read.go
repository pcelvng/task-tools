package file

import "github.com/pcelvng/task-tools/file/noop"

func NewStatsReader(pth string, _ *FileConfig) (StatsReader, error) {
	return noop.NewReader(pth), nil
}
