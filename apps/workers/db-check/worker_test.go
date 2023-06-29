package main

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hydronica/trial"
	"github.com/jmoiron/sqlx"
)

func TestGetZeroSums(t *testing.T) {
	type Row struct {
		integer int
		numeric string
		date    string // format 2006-01-02T15
	}

	type Input struct {
		DateType string
		Date     string
		GroupTS  string
		Rows     []Row
	}

	type Expected struct {
		ZR ZeroRecs
		MR MissingRecs
	}

	fn := func(in Input) (Expected, error) {
		// setup mock db response
		db, mock, _ := sqlmock.New()
		eq := mock.ExpectQuery("select *")
		column1 := mock.NewColumn("impression").OfType("INTEGER", int64(0))
		column2 := mock.NewColumn("revenue").OfType("NUMERIC", []uint8("0.0"))
		column3 := mock.NewColumn("date").OfType("TIMESTAMP", time.Time{})
		rows := mock.NewRowsWithColumnDefinition(column1, column2, column3)
		for _, r := range in.Rows {
			rows.AddRow(int64(r.integer), []uint8(r.numeric), trial.TimeHour(r.date))
		}
		eq.WillReturnRows(rows)

		w := &worker{
			options: options{Psql: map[string]Postgres{"test": {
				DB: sqlx.NewDb(db, "sql"),
			}}},
			DBSrc:    "test",
			DateType: in.DateType,
			Date:     in.Date,
			GroupTS:  in.GroupTS,
		}

		zr, mr, _ := w.GetZeroSums(context.Background())
		return Expected{ZR: zr, MR: mr}, nil
	}
	cases := trial.Cases[Input, Expected]{
		"timestamp_no_zero_missing": {
			Input: Input{DateType: "ts", Date: "2006-01-02T14:00", GroupTS: "", Rows: []Row{
				{123, "123.45", "2006-01-02T14"},
			}},
			Expected: Expected{
				ZR: ZeroRecs{},
				MR: MissingRecs{},
			},
		},
		"timestamp_zero_integer_numeric": {
			Input: Input{DateType: "ts", Date: "2006-01-02T14:00", GroupTS: "", Rows: []Row{
				{0, "0", "2006-01-02T14"},
			}},
			Expected: Expected{
				ZR: ZeroRecs{
					{Field: "impression", Hour: 14},
					{Field: "revenue", Hour: 14},
				},
				MR: MissingRecs{},
			},
		},
		"timestamp_missing_record": {
			Input: Input{DateType: "ts", Date: "2006-01-02T14:00", GroupTS: "", Rows: []Row{}},
			Expected: Expected{
				ZR: ZeroRecs{},
				MR: MissingRecs{{Hour: 14}},
			},
		},
		"date_group_ts_zero_int_num_missing": {
			Input: Input{DateType: "dt", Date: "2006-01-02", GroupTS: "timestamp-field", Rows: []Row{
				{123, "123.45", "2006-01-02T00"},
				{123, "123.45", "2006-01-02T01"},
				{123, "123.45", "2006-01-02T02"},
				{123, "0", "2006-01-02T04"},
				{123, "123.45", "2006-01-02T06"},
				{123, "123.45", "2006-01-02T07"},
				{123, "123.45", "2006-01-02T08"},
				{123, "123.45", "2006-01-02T09"},
				{123, "123.45", "2006-01-02T10"},
				{0, "0", "2006-01-02T11"},
				{123, "123.45", "2006-01-02T12"},
				{123, "123.45", "2006-01-02T13"},
				{123, "123.45", "2006-01-02T15"},
				{123, "123.45", "2006-01-02T16"},
				{123, "123.45", "2006-01-02T17"},
				{0, "123.45", "2006-01-02T18"},
				{123, "123.45", "2006-01-02T19"},
				{123, "123.45", "2006-01-02T22"},
				{123, "123.45", "2006-01-02T23"},
			}},
			Expected: Expected{
				ZR: ZeroRecs{
					{Field: "revenue", Hour: 4},
					{Field: "impression", Hour: 11},
					{Field: "revenue", Hour: 11},
					{Field: "impression", Hour: 18},
				},
				MR: MissingRecs{
					{Hour: 3},
					{Hour: 5},
					{Hour: 14},
					{Hour: 20},
					{Hour: 21},
				},
			},
		},
		"date_no_zero_missing": {
			Input: Input{DateType: "dt", Date: "2006-01-02", GroupTS: "", Rows: []Row{
				{123, "123.45", "2006-01-02T00"},
			}},
			Expected: Expected{
				ZR: ZeroRecs{},
				MR: MissingRecs{},
			},
		},
		"date_zero_integer_numeric": {
			Input: Input{DateType: "dt", Date: "2006-01-02", GroupTS: "", Rows: []Row{
				{0, "0", "2006-01-02T00"},
			}},
			Expected: Expected{
				ZR: ZeroRecs{
					{Field: "impression", Hour: 0},
					{Field: "revenue", Hour: 0},
				},
				MR: MissingRecs{},
			},
		},
		"date_missing_record": {
			Input: Input{DateType: "dt", Date: "2006-01-02", GroupTS: "", Rows: []Row{}},
			Expected: Expected{
				ZR: ZeroRecs{},
				MR: MissingRecs{{Hour: 0}},
			},
		},
	}

	trial.New(fn, cases).Test(t)
}
