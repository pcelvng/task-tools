package noop

import (
	"github.com/pcelvng/task-tools/file/stat"
)

func NewReader() *Reader {
	return &Reader{}
}

type Reader struct {}

func (w *Reader) Close() error {return nil}

func (w *Reader) ReadLine() ([]byte, error) {return nil, nil}

func (w *Reader) Stats() *stat.Stat {
	return stat.NewStat()
}