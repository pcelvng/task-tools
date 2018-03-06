package nop

import (
	"context"
	"errors"

	"github.com/pcelvng/task-tools/db/stat"
)

func NewBatchLoader(mode string) *BatchLoader {
	return &BatchLoader{
		Mode: mode,
	}
}

type BatchLoader struct {
	// Modes supported:
	// commit_err - will return an error on calling Commit
	Mode string

	Stats stat.Stats
}

func (l *BatchLoader) Delete(query string, vals ...interface{}) {}

func (l *BatchLoader) AddRow(row []interface{}) {
	l.Stats.AddRow()
}

func (l *BatchLoader) Commit(ctx context.Context, tableName string, cols ...string) (stat.Stats, error) {
	if l.Mode == "commit_err" {
		return l.Stats.Clone(), errors.New(l.Mode)
	}

	return l.Stats.Clone(), nil
}
