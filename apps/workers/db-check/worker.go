package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/slack"
)

type worker struct {
	options
	task.Meta
	slack *slack.Slack

	DB        string        `uri:"db" required:"true"`                       // the name of the database connection
	Table     string        `uri:"table" required:"true"`                    // name of the table to query
	DtColumn  string        `uri:"dt_column" required:"true"`                // the date column to query for row counts
	Date      time.Time     `uri:"date" format:"2006-01-02" required:"true"` // the date value to compare
	Offset    time.Duration `uri:"offset"`                                   // duration back from the date to compare records
	Tolerance float64       `uri:"tolerance" default:"0.05"`                 // allowed difference between date and offset date row counts
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

	return w
}

// RecordCount returns the row count for the given column field, and date
func (w *worker) RecordCount(ctx context.Context, field string, date time.Time) (rows int, err error) {
	c, found := w.Psql[w.DB]
	if !found {
		return 0, fmt.Errorf("db name %s not found", w.DB)
	}
	q := "select count(0) r from " + w.Table + " where date(" + field + ") = $1"
	err = c.DB.GetContext(ctx, &rows, q, date)
	if err != nil {
		return rows, fmt.Errorf("error getting record count from table %w", err)
	}
	return rows, nil
}

// RecordSum returns a summation of the given column sumField by the DtColumn provided
func (w *worker) RecordSum(ctx context.Context, DtColumn, sumField string, date time.Time) (sum float64, err error) {
	c, found := w.Psql[w.DB]
	if !found {
		return 0, fmt.Errorf("db name %s not found", w.DB)
	}
	q := fmt.Sprintf("select sum(%s) r from "+w.Table+" where date("+DtColumn+") = $1", sumField)
	err = c.DB.GetContext(ctx, &sum, q, date)
	if err != nil {
		return sum, fmt.Errorf("error getting record count from table %w", err)
	}
	return sum, nil
}

// Gets the row count from the table by DtColumn
// verifies that there are records for Date1
// if Date2 or Offset is provided compares row counts between the two dates
func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	records, err := w.RecordCount(ctx, w.DtColumn, w.Date)
	if err != nil {
		return task.Failed(err)
	}

	if records == 0 {
		m := []string{"no records found",
			fmt.Sprintf("column `%s` value `%s`", w.DtColumn, w.Date.Format("2006-01-02")),
		}
		w.sendSlack(m...)
		return task.Completed(fmt.Sprintln(m))
	}

	var p float64
	var msg string
	if w.Offset != 0 {
		dtCheck, err := date(w.Date, w.Offset)
		if err != nil {
			return task.Failed(err)
		}

		records2, err := w.RecordCount(ctx, w.DtColumn, dtCheck)
		if err != nil {
			return task.Failed(err)
		}
		if records2 == 0 {
			m := []string{"no comparison date records found",
				fmt.Sprintf("column `%s` value `%s`", w.DtColumn, dtCheck.Format("2006-01-02")),
			}
			w.sendSlack(m...)
			return task.Completed(fmt.Sprintln(m))
		}
		p = (float64(records2) - float64(records)) / float64(records)
		if math.Abs(p) > w.Tolerance {
			m := []string{
				"record count unexpected",
				fmt.Sprintf("`%s` has %d rows", w.Date.Format("2006-01-02"), records),
				fmt.Sprintf("`%s`, has %d rows", dtCheck.Format("2006-01-02"), records2),
				fmt.Sprintf("%.3f > %.3f", math.Abs(p)*100, w.Tolerance*100),
			}
			w.sendSlack(m...)
			return task.Completed(fmt.Sprintln(m))
		}

		msg = fmt.Sprintf("tolarence good at %.2f%%  %s:%d ~ %s:%d",
			p*100.0, w.Date.Format("2006-01-02"), records, dtCheck.Format("2006-01-02"), records2)
	}

	return task.Completed("table %s %s db-check passed for date %s, records: %d %s",
		w.Table, w.DtColumn, w.Date.Format("2006-01-02"), records, msg)
}

// formats messages and sends the message to slack
func (w *worker) sendSlack(msg ...string) {
	log.Println("sending slack alert message")
	tm := fmt.Sprintf(":heavy_exclamation_mark: *task-tools db-check* `%s`\n", w.Table)
	for _, m := range msg {
		tm += ("•         " + m + "\n")
	}
	sm := w.slack.NewMessage(tm)
	err := w.slack.SendMessage(sm)
	if err != nil {
		log.Println("error sending slack message", err.Error())
	}
}

// date will take a given time.Time and duration and return the time minus that duration
// truncated to just the date i.e., 2006-01-02
func date(dt time.Time, duration time.Duration) (t time.Time, err error) {
	if dt.IsZero() {
		return t, fmt.Errorf("zero time given")
	}

	t = time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC).Add(-duration)
	return t, nil
}
