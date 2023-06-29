package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sort"
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

	DBSrc     string `uri:"db_src" required:"true"`     // database source
	Table     string `uri:"table" required:"true"`      // name of the schema.table to query
	Type      string `uri:"type" required:"true"`       // type of check
	Field     string `uri:"field"`                      // field name being checked
	DateField string `uri:"date_field" required:"true"` // date/time field to query
	DateType  string `uri:"date_type"`                  // date type ("dt" = date, "ts" = timestamp)
	Date      string `uri:"date" required:"true"`       // date value to use in query
	GroupTS   string `uri:"group_ts"`                   // date field to group by
}

type ZeroRec struct {
	Field string
	Hour  int
}

type MissingRec struct {
	Hour int
}

type ZeroRecs []ZeroRec
type MissingRecs []MissingRec

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
	if w.Type == "zero" && w.DateType == "ts" {
		_, err := time.Parse("2006-01-02T15:00", w.Date)
		if err != nil {
			return task.InvalidWorker("invalid date; expected format: yyyy-mm-ddThh:00")
		}
	} else {
		_, err := time.Parse("2006-01-02", w.Date)
		if err != nil {
			return task.InvalidWorker("invalid date; expected format: yyyy-mm-dd")
		}
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
	m := w.slack.NewMessage(":radioactive_sign: *task-tools db-check - Missing Data Check - " + w.Date + "*")
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
		log.Printf("(%s) %s : %s missing data \n", w.DBSrc, w.Table, w.Date)
	}

	// for any issues, send slack message
	if issues > 0 {
		w.slack.SendMessage(m)
	}

	return task.Completed("table %s missing data check completed for date %s, issues: %d", w.Table, w.Date, issues)
}

func (w *worker) GetRecordCount(ctx context.Context) (count int64, err error) {
	pg, found := w.Psql[w.DBSrc]
	if !found {
		return 0, fmt.Errorf("db source %s not found", w.DBSrc)
	}
	qStr := fmt.Sprintf("select count(0) as count from %s where date(%s) = '%s'", w.Table, w.DateField, w.Date)
	row := pg.DB.QueryRowxContext(ctx, qStr)
	err = row.Scan(&count)
	if err != nil {
		return 0, errors.Wrap(err, "postgres scan")
	}
	return count, nil
}

func (w *worker) CheckNull(ctx context.Context) (task.Result, string) {
	m := w.slack.NewMessage(":radioactive_sign: *task-tools db-check - Null Check - " + w.Date + "*")
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
		log.Printf("null value: (%s) %s; %s %s\n", w.DBSrc, w.Table, w.Field, w.Date)
	}

	// for any issues, send slack message
	if issues > 0 {
		w.slack.SendMessage(m)
	}

	return task.Completed("table %s; field %s null check completed for date %s, issues: %d", w.Table, w.Field, w.Date, issues)
}

func (w *worker) GetNullCount(ctx context.Context) (count int64, err error) {
	pg, found := w.Psql[w.DBSrc]
	if !found {
		return 0, fmt.Errorf("db source %s not found", w.DBSrc)
	}
	qStr := fmt.Sprintf("select count(0) as count from %s where %s is null and date(%s) = '%s'", w.Table, w.Field, w.DateField, w.Date)
	row := pg.DB.QueryRowxContext(ctx, qStr)
	err = row.Scan(&count)
	if err != nil {
		return 0, errors.Wrap(err, "postgres scan")
	}
	return count, nil
}

func (w *worker) CheckZeroSum(ctx context.Context) (task.Result, string) {
	m := w.slack.NewMessage(":radioactive_sign: *task-tools db-check - Zero Sum Check - " + w.Date + "*")
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
				log.Printf("zero sum: (%s) %s; (hour: %d) %s %s\n", w.DBSrc, w.Table, r.Hour, r.Field, w.Date)
			}
		}
		if mr != nil {
			for _, r := range mr {
				// missing record(s) - send a slack message alerting for the table & missing hour
				issues++
				m.AddElements(fmt.Sprintf(":no_entry_sign:  *(%s) %s; (Hour: %d)* - missing record(s)", w.DBSrc, w.Table, r.Hour))
				log.Printf("missing record(s): (%s) %s; (hour: %d) %s\n", w.DBSrc, w.Table, r.Hour, w.Date)
			}
		}
	}

	// for any issues, send slack message
	if issues > 0 {
		w.slack.SendMessage(m)
	}

	return task.Completed("table %s; zero sum check completed for date %s, issues: %d", w.Table, w.Date, issues)
}

func (w *worker) GetZeroSums(ctx context.Context) (zr ZeroRecs, mr MissingRecs, err error) {
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
	foundHours := make([]bool, 24)

	qStr := fmt.Sprintf("select %s, %s as date from %s where %s = '%s' %s;", selString, asDate, w.Table, w.DateField, w.Date, grpString)
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
	if w.GroupTS != "" {
		for hour, found := range foundHours {
			if !found {
				mr = append(mr, MissingRec{Hour: hour})
			}
		}
	} else {
		selHour := 0
		s := strings.Split(w.Date, "T")
		if len(s) > 1 {
			selHour, _ = strconv.Atoi(s[1][0:2])
		}
		if !foundHours[selHour] {
			mr = append(mr, MissingRec{Hour: selHour})
		}
	}
	sort.Sort(zr) //ensures hour->field order in slack messages and for unit tests
	return zr, mr, nil
}

func (z ZeroRecs) Len() int      { return len(z) }
func (z ZeroRecs) Swap(i, j int) { z[i], z[j] = z[j], z[i] }
func (z ZeroRecs) Less(i, j int) bool {
	if z[i].Hour == z[j].Hour {
		return strings.ToLower(z[i].Field) < strings.ToLower(z[j].Field)
	}
	return z[i].Hour < z[j].Hour
}
