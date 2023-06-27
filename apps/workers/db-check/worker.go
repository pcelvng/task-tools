package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
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

	DBSrc     string    `uri:"db_src" required:"true"`                   // database source
	Table     string    `uri:"table" required:"true"`                    // name of the schema.table to query
	Type      string    `uri:"type" required:"true"`                     // type of check
	Field     string    `uri:"field"`                                    // field name being checked
	DateField string    `uri:"date_field" required:"true"`               // date/time field to query
	DateType  string    `uri:"date_type"`                                // date type ("dt" = date, "ts" = timestamp)
	Date      time.Time `uri:"date" format:"2006-01-02" required:"true"` // date value to use in query
	GroupTS   string    `uri:"group_ts"`                                 // date field to group by
}

type ZeroRec struct {
	Field string
	Hour  int
}

type MissingRec struct {
	Hour int
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
	if (w.Type == "null" || w.Type == "zero") && w.Field == "" {
		return task.InvalidWorker("field is required on %s checks", w.Type)
	}
	if w.Type == "zero" && w.DateType != "dt" && w.DateType != "ts" {
		return task.InvalidWorker("date_type (dt|ts) is required on zero checks")
	}
	if w.Type == "zero" && w.DateType == "ts" && w.GroupTS != "" {
		return task.InvalidWorker("group_ts only valid with 'dt' date_type")
	}

	return w
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	switch w.Type {
	case "missing":
		return w.CheckMissing(ctx)
	case "null":
		return w.CheckNull(ctx)
	case "zero":
		return w.CheckZeroSum(ctx)
	default:
		return task.Failf("unsupported type %s", w.Type)
	}
}

func (w *worker) CheckMissing(ctx context.Context) (task.Result, string) {
	d := w.Date.Format("2006-01-02")
	m := w.slack.NewMessage(":radioactive_sign: *task-tools db-check - Missing Data Check - " + d + "*")
	issues := 0

	cnt, err := w.GetRecordCount(ctx)
	if err != nil {
		issues++
		// there was an error when checking table data add a slack message block with the error
		m.AddElements(fmt.Sprintf(":octagonal_sign:  *(%s) %s* - %s", w.DBSrc, w.Table, err.Error()))
		log.Printf("(%s) %s - %s\n", w.DBSrc, w.Table, err.Error())
	} else if cnt == 0 {
		// there are no records, or nothing to process in the table
		// send a slack message alerting for the table
		issues++
		m.AddElements(fmt.Sprintf(":no_entry_sign:  *(%s) %s* - missing data", w.DBSrc, w.Table))
		log.Printf("(%s) %s : %s missing data \n", w.DBSrc, w.Table, d)
	}

	// for any issues, send slack message
	if issues > 0 {
		w.slack.SendMessage(m)
	}

	return task.Completed("table %s missing data check completed for date %s, issues: %d", w.Table, d, issues)
}

func (w *worker) GetRecordCount(ctx context.Context) (count int64, err error) {
	pg, found := w.Psql[w.DBSrc]
	if !found {
		return 0, fmt.Errorf("db source %s not found", w.DBSrc)
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

	cnt, err := w.GetNullCount(ctx)
	if err != nil {
		issues++
		// there was an error when checking table data add a slack message block with the error
		m.AddElements(fmt.Sprintf(":octagonal_sign:  *(%s) %s; %s* - %s", w.DBSrc, w.Table, w.Field, err.Error()))
		log.Printf("(%s) %s; %s - %s\n", w.DBSrc, w.Table, w.Field, err.Error())
	} else if cnt != 0 {
		// null value found - send a slack message alerting for the table & field
		issues++
		m.AddElements(fmt.Sprintf(":no_entry_sign:  *(%s) %s; %s* - null value", w.DBSrc, w.Table, w.Field))
		log.Printf("null value: (%s) %s; %s %s\n", w.DBSrc, w.Table, w.Field, d)
	}

	// for any issues, send slack message
	if issues > 0 {
		w.slack.SendMessage(m)
	}

	return task.Completed("table %s; field %s null check completed for date %s, issues: %d", w.Table, w.Field, d, issues)
}

func (w *worker) GetNullCount(ctx context.Context) (count int64, err error) {
	pg, found := w.Psql[w.DBSrc]
	if !found {
		return 0, fmt.Errorf("db source %s not found", w.DBSrc)
	}
	qStr := fmt.Sprintf("select count(0) as count from %s where %s is null and date(%s) = '%s'", w.Table, w.Field, w.DateField, w.Date.Format("2006-01-02"))
	row := pg.DB.QueryRowxContext(ctx, qStr)
	err = row.Scan(&count)
	if err != nil {
		return 0, errors.Wrap(err, "postgres scan")
	}
	return count, nil
}

func (w *worker) CheckZeroSum(ctx context.Context) (task.Result, string) {
	d := w.Date.Format("2006-01-02")
	m := w.slack.NewMessage(":radioactive_sign: *task-tools db-check - Zero Sum Check - " + d + "*")
	issues := 0

	zr, mr, err := w.GetZeroSums(ctx)
	if err != nil {
		issues++
		// there was an error when checking table data add a slack message block with the error
		m.AddElements(fmt.Sprintf(":octagonal_sign:  *(%s) %s; %s", w.DBSrc, w.Table, err.Error()))
		log.Printf("(%s) %s; %s\n", w.DBSrc, w.Table, err.Error())
	} else {
		if zr != nil {
			for _, r := range zr {
				// zero sum found - send a slack message alerting for the table, hour and field
				issues++
				m.AddElements(fmt.Sprintf(":no_entry_sign:  *(%s) %s; (Hour: %d) %s* - zero sum", w.DBSrc, w.Table, r.Hour, r.Field))
				log.Printf("zero sum: (%s) %s; (hour: %d) %s %s\n", w.DBSrc, w.Table, r.Hour, r.Field, d)
			}
		}
		if mr != nil {
			for _, r := range mr {
				// missing record(s) - send a slack message alerting for the table & missing hour
				issues++
				m.AddElements(fmt.Sprintf(":no_entry_sign:  *(%s) %s; (Hour: %d)* - missing record(s)", w.DBSrc, w.Table, r.Hour))
				log.Printf("missing record(s): (%s) %s; (hour: %d) %s\n", w.DBSrc, w.Table, r.Hour, d)
			}
		}
	}

	// for any issues, send slack message
	if issues > 0 {
		w.slack.SendMessage(m)
	}

	return task.Completed("table %s; zero sum check completed for date %s, issues: %d", w.Table, d, issues)
}

func (w *worker) GetZeroSums(ctx context.Context) (zr []ZeroRec, mr []MissingRec, err error) {
	pg, found := w.Psql[w.DBSrc]
	if !found {
		return nil, nil, fmt.Errorf("db source %s not found", w.DBSrc)
	}

	selString := ""
	s := strings.Split(strings.Replace(w.Field, " ", "", -1), ",")
	for _, v := range s {
		selString += fmt.Sprintf("sum(%s) as %s,", v, v)
	}
	selString = selString[:len(selString)-1] //remove last comma

	asDate := ""
	grpString := ""
	if w.GroupTS == "" {
		grpString = fmt.Sprintf("group by %s order by %s", w.DateField, w.DateField)
		asDate = w.DateField
	} else {
		grpString = fmt.Sprintf("group by %s order by %s", w.GroupTS, w.GroupTS)
		asDate = w.GroupTS
	}
	hrCnt := 1
	if w.DateType == "ts" || w.GroupTS != "" {
		hrCnt = 24
	}
	foundHours := make([]bool, hrCnt)

	qStr := fmt.Sprintf("select %s, %s as date from %s where date(%s) = '%s' %s;", selString, asDate, w.Table, w.DateField, w.Date.Format("2006-01-02"), grpString)
	rows, err := pg.DB.QueryxContext(ctx, qStr)
	if err != nil {
		return nil, nil, errors.Wrap(err, "postgres query")
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, nil, errors.Wrap(err, "postgres columns")
	}
	var colVals = make(map[string]interface{})
	cols := make([]interface{}, len(colNames))
	colPtrs := make([]interface{}, len(colNames))
	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}
	for rows.Next() {
		err = rows.Scan(colPtrs...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "postgres scan")
		}
		for i, col := range cols {
			colVals[colNames[i]] = col
		}
		hour := 0
		zeroCols := make([]string, 0)
		for cName, cVal := range colVals {
			valType := reflect.TypeOf(cVal)
			switch vt := valType.String(); vt {
			case "int64":
				if cVal.(int64) == 0 {
					zeroCols = append(zeroCols, cName)
				}
			case "[]uint8":
				v, _ := strconv.ParseFloat(string(cVal.([]uint8)), 64)
				if v == 0 {
					zeroCols = append(zeroCols, cName)
				}
			case "time.Time":
				hour = cVal.(time.Time).Hour()
				foundHours[hour] = true
			default:
				return nil, nil, fmt.Errorf("unsupported column type %s", vt)
			}
		}
		for _, cName := range zeroCols {
			zr = append(zr, ZeroRec{Field: cName, Hour: hour})
		}
	}

	for hour, found := range foundHours {
		if !found {
			mr = append(mr, MissingRec{Hour: hour})
		}
	}
	return zr, mr, nil
}
