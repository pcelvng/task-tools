package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/slack"
	"github.com/pkg/errors"
)

type worker struct {
	options
	task.Meta
	slack *slack.Slack

	DBSrv     string    `uri:"db_srv" required:"true"`                   // database server/source connection
	Table     string    `uri:"table" required:"true"`                    // name of the schema.table to query
	Type      string    `uri:"type" required:"true"`                     // type of check
	Field     string    `uri:"field"`                                    // field name being checked
	DateField string    `uri:"date_field" required:"true"`               // date/time field to query
	Date      time.Time `uri:"date" format:"2006-01-02" required:"true"` // date value to use in query
}

func (o *options) newWorker(info string) task.Worker {
	w := &worker{
		options: *o, // copy option values
		Meta:    task.NewMeta(),
		slack:   &slack.Slack{Url: o.Slack},
	}

	if err := uri.Unmarshal(info, w); err != nil {
		return task.InvalidWorker("params uri.unmarshal: %v", err)
	}

	if w.Type == "null" && w.Field == "" {
		return task.InvalidWorker("field is required on null checks")
	}

	return w
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	switch w.Type {
	case "missing":
		return w.CheckMissing(ctx)
	case "null":
		return w.CheckNull(ctx)
	default:
		return task.Failf("unsupported type %s", w.Type)
	}
}

func (w *worker) CheckMissing(ctx context.Context) (task.Result, string) {
	d := w.Date.Format("2006-01-02")
	m := w.slack.NewMessage(":radioactive_sign: *task-tools db-check - Missing Data Check - " + d + "*")
	issues := 0

	c, err := w.GetRecordCount(ctx)
	if err != nil {
		issues++
		// there was an error when checking table data add a slack message block with the error
		m.AddElements(fmt.Sprintf(":octagonal_sign:  *%s* - %s", w.Table, err.Error()))
		log.Printf("%s - %s\n", w.Table, err.Error())
	} else {
		// there are no records, or nothing to process in the table
		// send a slack message alerting for the table
		if c == 0 {
			issues++
			m.AddElements(fmt.Sprintf(":no_entry_sign:  *%s* - missing data", w.Table))
			log.Printf("%s : %s missing data \n", w.Table, d)
		}
	}

	// for any issues, send slack message
	if issues > 0 {
		w.slack.SendMessage(m)
	}

	return task.Completed("table %s missing data check completed for date %s, issues: %d", w.Table, d, issues)
}

func (w *worker) GetRecordCount(ctx context.Context) (count int64, err error) {
	pg, found := w.Psql[w.DBSrv]
	if !found {
		return 0, fmt.Errorf("db name %s not found", w.DBSrv)
	}
	qStr := fmt.Sprintf("select count(0) as count from %s where date(%s) = '%s'", w.Table, w.DateField, w.Date.Format("2006-01-02"))
	row := pg.DB.QueryRowxContext(ctx, qStr)
	err = row.Scan(&count)
	if err != nil {
		return 0, errors.Wrap(err, "postgres scan")
	}
	return count, nil
}

func (w *worker) CheckNull(ctx context.Context) (task.Result, string) {
	d := w.Date.Format("2006-01-02")
	m := w.slack.NewMessage(":radioactive_sign: *task-tools db-check - Null Check - " + d + "*")
	issues := 0

	c, err := w.GetNullCount(ctx)
	if err != nil {
		issues++
		// there was an error when checking table data add a slack message block with the error
		m.AddElements(fmt.Sprintf(":octagonal_sign:  *%s; %s* - %s", w.Table, w.Field, err.Error()))
		log.Printf("%s; %s - %s\n", w.Table, w.Field, err.Error())
	} else {
		// null value found - send a slack message alerting for the table & field
		if c != 0 {
			issues++
			m.AddElements(fmt.Sprintf(":no_entry_sign:  *%s; %s* - null value", w.Table, w.Field))
			log.Printf("null value: %s; %s %s\n", w.Table, w.Field, d)
		}
	}

	// for any issues, send slack message
	if issues > 0 {
		w.slack.SendMessage(m)
	}

	return task.Completed("table %s; field %s null check completed for date %s, issues: %d", w.Table, w.Field, d, issues)
}

func (w *worker) GetNullCount(ctx context.Context) (count int64, err error) {
	pg, found := w.Psql[w.DBSrv]
	if !found {
		return 0, fmt.Errorf("db name %s not found", w.DBSrv)
	}
	qStr := fmt.Sprintf("select count(0) as count from %s where %s is null and date(%s) = '%s'", w.Table, w.Field, w.DateField, w.Date.Format("2006-01-02"))
	row := pg.DB.QueryRowxContext(ctx, qStr)
	err = row.Scan(&count)
	if err != nil {
		return 0, errors.Wrap(err, "postgres scan")
	}
	return count, nil
}
