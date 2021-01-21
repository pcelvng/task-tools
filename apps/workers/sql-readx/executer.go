package main

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/pcelvng/task"
)

type executer struct {
	task.Meta
	db     *sqlx.DB
	Query  string
	Fields []interface{}
}

func (w *executer) DoTask(ctx context.Context) (task.Result, string) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return task.Failed(err)
	}
	r, err := w.db.ExecContext(ctx, w.Query, w.Fields...)
	if err != nil {
		tx.Rollback()
		return task.Failed(err)
	}
	if err = tx.Commit(); err != nil {
		return task.Failed(err)
	}
	id, _ := r.LastInsertId()
	rows, _ := r.RowsAffected()
	return task.Completed("done %d with %d rows affected", id, rows)
}
