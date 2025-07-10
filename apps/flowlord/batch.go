package main

import (
	"time"

	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/file"
)

type batch struct {
	For      time.Duration       `uri:"for"`
	By       string              `uri:"by"`
	Meta     map[string][]string `uri:"meta"`
	FilePath string              `uri:"meta-file"`
}

func (b *batch) Batch(t time.Time, fOpts file.Options) ([]*task.Task, error) {
	return nil, nil
}
