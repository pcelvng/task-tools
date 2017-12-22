package noop

import (
	"github.com/pcelvng/task-tools/file/stat"
)

func NewWriter(pth string) *Writer {
	return &Writer{
		pth: pth,
	}
}

type Writer struct {
	pth string
}

func (w *Writer) Close() error { return nil }

func (w *Writer) WriteLine([]byte) (int64, error) { return 0, nil }

func (w *Writer) Finish() error { return nil }

func (w *Writer) Stats() *stat.Stat {
	stats := stat.NewStat()
	stats.Path = w.pth
	return stats
}
