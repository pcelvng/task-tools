package nop

import (
	"github.com/pcelvng/task-tools/file/stat"
)

func NewReader(pth string) *Reader {
	return &Reader{
		pth: pth,
	}
}

type Reader struct {
	pth string
}

func (w *Reader) Close() error { return nil }

func (w *Reader) ReadLine() ([]byte, error) { return nil, nil }

func (w *Reader) Stats() *stat.Stat {
	stats := stat.NewStat()
	stats.Path = w.pth
	return stats
}
