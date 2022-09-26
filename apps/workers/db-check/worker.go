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

	Table     string    `uri:"table" required:"true"`      // name of the table to query
	DateField string    `uri:"date_field" required:"true"` // the date field to get row counts
	Date1     time.Time `uri:"date1" format:"2006-01-02"`  // default is 24h back from today
	Date2     time.Time `uri:"date2" format:"2006-01-02"`  // 2nd date to compare records against date1
	Offset    string    `uri:"offset"`                     // duration back from date1 used to determine the value of date2 if needed
	Percent   float64   `uri:"percent" default:"0.05"`     // allowed difference between date1 and date2 row counts
}

func (o *options) newWorker(info string) task.Worker {
	w := &worker{
		options: *o, // copy option values
		Meta:    task.NewMeta(),
		Date1:   time.Now().Add(-time.Hour * 24), // default date is yesterday
		slack:   &slack.Slack{Url: o.Slack},
	}

	if err := uri.Unmarshal(info, w); err != nil {
		return task.InvalidWorker("params uri.unmarshal: %v", err)
	}

	return w
}

// RecordCount returns the row count for the given column field, and date
func (w *worker) RecordCount(ctx context.Context, field string, date time.Time) (rows int, err error) {
	q := "select count(0) r from " + w.Table + " where date(" + field + ") = $1"
	err = w.Psql.DB.GetContext(ctx, &rows, q, date)
	if err != nil {
		return rows, fmt.Errorf("error getting record count from table %w", err)
	}
	return rows, nil
}

// RecordSum returns a summation of the given column sumField by the dateField provided
func (w *worker) RecordSum(ctx context.Context, dateField, sumField string, date time.Time) (sum float64, err error) {
	q := fmt.Sprintf("select sum(%s) r from "+w.Table+" where date("+dateField+") = $1", sumField)
	err = w.Psql.DB.GetContext(ctx, &sum, q, date)
	if err != nil {
		return sum, fmt.Errorf("error getting record count from table %w", err)
	}
	return sum, nil
}

// Gets the row count from the table by DateField
// verifies that there are records for Date1
// if Date2 or Offset is provided compares row counts between the two dates
func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	records, err := w.RecordCount(ctx, w.DateField, w.Date1)
	if err != nil {
		return task.Failed(err)
	}

	if records == 0 {
		m := []string{"no records found",
			fmt.Sprintf("no `%s` for `%s`", w.DateField, w.Date1.Format("2006-01-02")),
		}
		w.sendSlack(m...)
		return task.Completed(fmt.Sprintln(m))
	}

	var p float64
	if w.Offset != "" {
		w.Date2, err = date(w.Date1, w.Offset)
		if err != nil {
			return task.Failed(err)
		}
	}
	var msg string
	if !w.Date2.IsZero() {
		records2, err := w.RecordCount(ctx, w.DateField, w.Date2)
		if err != nil {
			return task.Failed(err)
		}

		if records2 == 0 {
			m := []string{"no comparison date records found",
				fmt.Sprintf("no `%s` for `%s`", w.DateField, w.Date2.Format("2006-01-02")),
			}
			w.sendSlack(m...)
			return task.Completed(fmt.Sprintln(m))
		}

		// if the change between compare and offset record counts is greather than the given percentage
		// send an alert to slack that the variance is greater than expected
		p = (float64(records2) - float64(records)) / float64(records)
		if math.Abs(p) > w.Percent {
			m := []string{
				"record count unexpected",
				fmt.Sprintf("`%s` has %d rows", w.Date1.Format("2006-01-02"), records),
				fmt.Sprintf("`%s`, has %d rows", w.Date2.Format("2006-01-02"), records2),
				fmt.Sprintf("%.3f%% > %.3f%%", math.Abs(p)*100, w.Percent*100),
			}
			w.sendSlack(m...)
			return task.Completed(fmt.Sprintln(m))
		}

		msg = fmt.Sprintf("variance good at %.2f%%  %s:%d ~ %s:%d",
			p*100.0, w.Date1.Format("2006-01-02"), records, w.Date2.Format("2006-01-02"), records2)
	}

	return task.Completed("table %s %s db-check passed for date %s, records: %d %s",
		w.Table, w.DateField, w.Date1.Format("2006-01-02"), records, msg)
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
func date(dt time.Time, duration string) (t time.Time, err error) {
	if dt.IsZero() {
		return t, fmt.Errorf("zero time given")
	}

	d, err := time.ParseDuration(duration)
	if err != nil {
		return t, fmt.Errorf("error parsing duration %s %w", duration, err)
	}

	t = time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC).Add(-d)
	return t, nil
}

func truncDate(date string) (t time.Time) {
	t, _ = time.Parse("2006-01-02", date)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
