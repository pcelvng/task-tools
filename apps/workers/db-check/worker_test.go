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
		IntCol  int64
		NumCol  []uint8
		DateCol time.Time
	}

	type Input struct {
		DateType string
		GroupTS  string
		Rows     []Row
	}

	type Expected struct {
		ZR ZeroRecs
		MR MissingRecs
	}

	d := time.Date(2023, 06, 15, 0, 0, 0, 0, time.UTC)
	dt := make([]time.Time, 24)
	for i := range dt {
		dt[i] = d.Add(time.Hour * time.Duration(i))
	}

	fn := func(in Input) (Expected, error) {
		// setup mock db response
		db, mock, _ := sqlmock.New()
		eq := mock.ExpectQuery("select *")
		column1 := mock.NewColumn("impression").OfType("INTEGER", int64(0))
		column2 := mock.NewColumn("revenue").OfType("NUMERIC", []uint8("0.0"))
		column3 := mock.NewColumn("date").OfType("TIMESTAMP", time.Time{})
		rows := mock.NewRowsWithColumnDefinition(column1, column2, column3)
		for i := range in.Rows {
			rows.AddRow(in.Rows[i].IntCol, in.Rows[i].NumCol, in.Rows[i].DateCol)
		}
		eq.WillReturnRows(rows)

		w := &worker{
			options: options{Psql: map[string]Postgres{"test": {
				DB: sqlx.NewDb(db, "sql"),
			}}},
			DBSrc:    "test",
			DateType: in.DateType,
			GroupTS:  in.GroupTS,
		}

		zr, mr, _ := w.GetZeroSums(context.Background())
		return Expected{ZR: zr, MR: mr}, nil
	}
	cases := trial.Cases[Input, Expected]{
		"timestamp_no_zero_missing": {
			Input: Input{DateType: "ts", GroupTS: "", Rows: []Row{
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[0]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[1]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[2]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[3]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[4]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[5]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[6]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[7]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[8]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[9]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[10]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[11]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[12]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[13]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[14]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[15]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[16]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[17]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[18]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[19]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[20]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[21]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[22]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[23]},
			}},
			Expected: Expected{
				ZR: ZeroRecs{},
				MR: MissingRecs{},
			},
		},
		"timestamp_zero_int_num_missing": {
			Input: Input{DateType: "ts", GroupTS: "", Rows: []Row{
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[0]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[1]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[2]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[4]},
				{IntCol: int64(0), NumCol: []uint8("123.45"), DateCol: dt[5]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[6]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[7]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[8]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[9]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[10]},
				{IntCol: int64(123), NumCol: []uint8("0"), DateCol: dt[11]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[12]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[13]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[15]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[16]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[17]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[18]},
				{IntCol: int64(0), NumCol: []uint8("0"), DateCol: dt[19]},
				{IntCol: int64(123), NumCol: []uint8("0"), DateCol: dt[20]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[22]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[23]},
			}},
			Expected: Expected{
				ZR: ZeroRecs{
					{Field: "impression", Hour: 5},
					{Field: "revenue", Hour: 11},
					{Field: "impression", Hour: 19},
					{Field: "revenue", Hour: 19},
					{Field: "revenue", Hour: 20},
				},
				MR: MissingRecs{
					{Hour: 3},
					{Hour: 14},
					{Hour: 21},
				},
			},
		},
		"date_group_ts_zero_int_num_missing": {
			Input: Input{DateType: "dt", GroupTS: "timestamp-field", Rows: []Row{
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[0]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[1]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[2]},
				{IntCol: int64(123), NumCol: []uint8("0"), DateCol: dt[4]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[6]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[7]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[8]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[9]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[10]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[11]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[12]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[13]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[15]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[16]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[17]},
				{IntCol: int64(0), NumCol: []uint8("123.45"), DateCol: dt[18]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[19]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[22]},
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[23]},
			}},
			Expected: Expected{
				ZR: ZeroRecs{
					{Field: "revenue", Hour: 4},
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
			Input: Input{DateType: "dt", GroupTS: "", Rows: []Row{
				{IntCol: int64(123), NumCol: []uint8("123.45"), DateCol: dt[0]},
			}},
			Expected: Expected{
				ZR: ZeroRecs{},
				MR: MissingRecs{},
			},
		},
		"date_zero_integer_numeric": {
			Input: Input{DateType: "dt", GroupTS: "", Rows: []Row{
				{IntCol: int64(0), NumCol: []uint8("0"), DateCol: dt[0]},
			}},
			Expected: Expected{
				ZR: ZeroRecs{
					{Field: "impression", Hour: 0},
					{Field: "revenue", Hour: 0},
				},
				MR: MissingRecs{},
			},
		},
		"date_missing_rec": {
			Input: Input{DateType: "dt", GroupTS: "", Rows: []Row{}},
			Expected: Expected{
				ZR: ZeroRecs{},
				MR: MissingRecs{{Hour: 0}},
			},
		},
	}

	trial.New(fn, cases).Test(t)
}
