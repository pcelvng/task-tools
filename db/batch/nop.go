package batch

import (
	"context"
	"errors"
)

func NewNopBatchLoader(mode string) *NopBatchLoader {
	return &NopBatchLoader{
		Mode: mode,
	}
}

type NopBatchLoader struct {
	// Modes supported:
	// commit_err - will return an error on calling Commit
	Mode string

	Stats Stats
}

func (l *NopBatchLoader) Delete(query string, vals ...interface{}) {}

func (l *NopBatchLoader) AddRow(row []interface{}) {
	l.Stats.AddRow()
}

func (l *NopBatchLoader) Commit(ctx context.Context, tableName string, cols ...string) (Stats, error) {
	if l.Mode == "commit_err" {
		return l.Stats.Clone(), errors.New(l.Mode)
	}

	return l.Stats.Clone(), nil
}
