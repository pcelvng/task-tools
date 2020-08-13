package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hydronica/trial"
	"github.com/jbsmith7741/go-tools/sqlh"
	"github.com/jmoiron/sqlx"
	"github.com/pcelvng/task"

	"github.com/pcelvng/task-tools/file/mock"
)

func TestNewWorker(t *testing.T) {
	mockDb := sqlx.MustOpen(sqlh.Mock, "mockDNS")
	tFile := "../../../internal/test/nop.sql"
	fn := func(in trial.Input) (interface{}, error) {
		opts := &options{
			db: mockDb,
		}
		w := opts.NewWorker(in.String())
		if invalid, s := task.IsInvalidWorker(w); invalid {
			return nil, errors.New(s)
		}

		return w.(*worker).Query, nil
	}
	cases := trial.Cases{
		"default": {
			Input:    tFile + "?table=schema.table&dest=nop://",
			Expected: "select * from fake_table;",
		},
		"lazy maps": {
			Input:    "?table=schema.table&dest=nop://&field=id:|name:|value:fruit",
			Expected: "select id, name, value from schema.table",
		},
		"no query": {
			Input:     "?table=schema.table&dest=nop://",
			ShouldErr: true,
		},
		"invalid table": {
			Input:       "?table=t&field=i:i&dest=nop://",
			ExpectedErr: errors.New("(schema.table)"),
		},
		"missing info params": {
			Input:     "",
			ShouldErr: true,
		},
		"writer err": {
			Input:       tFile + "?table=schema.table&dest=nop://init_err",
			ExpectedErr: errors.New("writer: "),
		},
	}
	trial.New(fn, cases).Timeout(3 * time.Second).SubTest(t)
}

func TestWorker_DoTask(t *testing.T) {
	type input struct {
		wPath  string
		fields FieldMap         // table definition
		Rows   [][]driver.Value // data returned from database
	}

	fn := func(i trial.Input) (interface{}, error) {
		in := i.Interface().(input)
		// setup mock db response
		db, mDB, _ := sqlmock.New()
		eq := mDB.ExpectQuery("select *")

		cols := make([]string, 0)
		for k := range in.fields {
			cols = append(cols, k)
		}
		// Add mock data
		rows := sqlmock.NewRows(cols)
		for _, d := range in.Rows {
			rows.AddRow(d...)
		}
		eq.WillReturnRows(rows)

		writer := mock.NewWriter(in.wPath)
		w := &worker{
			Meta:   task.NewMeta(),
			writer: writer,
			Fields: in.fields,
			db:     sqlx.NewDb(db, "sql"),
			Query:  "select *",
		}

		// return data written to file or an err on task failure
		r, s := w.DoTask(context.Background())
		if r == task.CompleteResult {
			return writer.GetLines(), nil
		} else {
			return nil, errors.New(s)
		}
	}
	cases := trial.Cases{
		"basic": {
			Input:    input{},
			Expected: []string{},
		},
		"good data": {
			Input: input{
				fields: FieldMap{"v": "fruit"},
				Rows: [][]driver.Value{
					{"apple"},
					{"banana"},
				},
			},
			Expected: []string{
				`{"fruit":"apple"}`,
				`{"fruit":"banana"}`,
			},
		},
		"lazy map": {
			Input: input{
				fields: FieldMap{"id": ""},
				Rows: [][]driver.Value{
					{1},
					{2},
				},
			},
			Expected: []string{
				`{"id":1}`,
				`{"id":2}`,
			},
		},
		"write fail": {
			Input: input{
				wPath:  "nop://writeline_err",
				fields: FieldMap{"id": "id", "v": "fruit"},
				Rows:   [][]driver.Value{{1, "apple"}},
			},
			ShouldErr: true,
		},
		"close err": {
			Input: input{
				wPath: "nop://err",
			},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).Test(t)

}
