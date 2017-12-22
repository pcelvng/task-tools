package noop

import (
	"github.com/pcelvng/task-tools/file/stat"
)

func NewWriter() *Writer {
	return &Writer{}
}

type Writer struct {}

func (w *Writer) Close() error {return nil}

func (w *Writer) WriteLine([]byte) (int64, error) {return 0, nil}

func (w *Writer) Finish() error {return nil}

func (w *Writer) Stats() *stat.Stat {
	return stat.NewStat()
}