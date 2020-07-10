package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
)

func TestVerifyRow(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		i := in.Interface().(*DataSet)
		err := i.VerifyRow()
		return i, err
	}

	// testing cases
	cases := trial.Cases{
		"empty_dataset_err": {
			Input:       &DataSet{},
			ExpectedErr: errors.New("no data found in json jRow object"),
		},
		"initial_missing_key": {
			Input: &DataSet{
				dbSchema: DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column2", IsNullable: "YES"}},
				jRow:     Jsondata{"column1": "column1datastring"},
			},
			Expected: &DataSet{
				dbSchema:   DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column2", IsNullable: "YES"}},
				jRow:       Jsondata{"column1": "column1datastring", "column2": nil},
				insertCols: []string{"column1", "column2"},
				verified:   true,
			},
		},
		"missing_db_column_ignored": {
			Input: &DataSet{
				dbSchema: DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column2", IsNullable: "YES"}},
				jRow:     Jsondata{"column1": "column1datastring", "column3": "column3datastring"},
			},
			Expected: &DataSet{
				dbSchema:   DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column2", IsNullable: "YES"}},
				jRow:       Jsondata{"column1": "column1datastring", "column3": "column3datastring", "column2": nil},
				insertCols: []string{"column1", "column2"},
				verified:   true,
			},
		},
		"initial_all_keys": {
			Input: &DataSet{
				dbSchema: DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column2", IsNullable: "YES"}},
				jRow:     Jsondata{"column1": "column1datastring", "column2": "column2datastring"},
			},
			Expected: &DataSet{
				dbSchema:   DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column2", IsNullable: "YES"}},
				jRow:       Jsondata{"column1": "column1datastring", "column2": "column2datastring"},
				insertCols: []string{"column1", "column2"},
				verified:   true,
			},
		},
		"missing_non_nullable_field": {
			Input: &DataSet{
				dbSchema: DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column2", IsNullable: "YES"}},
				jRow:     Jsondata{"column2": "column1datastring", "column3": "column3datastring"},
			},
			Expected: &DataSet{ // if not provided a non-nullable field is ignored, assuming that the DB will handle default values
				dbSchema:   DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column2", IsNullable: "YES"}},
				jRow:       Jsondata{"column2": "column1datastring", "column3": "column3datastring"},
				verified:   true,
				insertCols: []string{"column2"},
			},
		},
		"insert_columns_already_set": {
			Input: &DataSet{
				dbSchema:   DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column3", IsNullable: "YES"}},
				jRow:       Jsondata{"column1": "column1datastring", "column2": "column3datastring"},
				insertCols: []string{"column1", "column3"},
			},
			Expected: &DataSet{
				dbSchema:   DbSchema{DbColumn{Name: "column1", IsNullable: "NO"}, DbColumn{Name: "column3", IsNullable: "YES"}},
				jRow:       Jsondata{"column1": "column1datastring", "column2": "column3datastring", "column3": nil},
				insertCols: []string{"column1", "column3"},
				verified:   true,
			},
		},
		"all_nil_data": {
			Input: &DataSet{
				dbSchema: DbSchema{DbColumn{Name: "column5", IsNullable: "YES"}, DbColumn{Name: "column6", IsNullable: "YES"}},
				jRow:     Jsondata{"column1": "column1datastring"},
			},
			Expected: &DataSet{
				dbSchema:   DbSchema{DbColumn{Name: "column5", IsNullable: "YES"}, DbColumn{Name: "column6", IsNullable: "YES"}},
				jRow:       Jsondata{"column1": "column1datastring", "column5": nil, "column6": nil},
				insertCols: []string{"column5", "column6"},
				verified:   true,
			},
		},
	}

	trial.New(fn, cases).Test(t)
}

func TestAddRow(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		i := in.Interface().(*DataSet)
		err := i.AddRow()
		return i, err
	}

	// testing cases
	cases := trial.Cases{
		"adding_another_row": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column1", IsNullable: "NO", DataType: "varchar"},
					DbColumn{Name: "column3", IsNullable: "YES", DataType: "character varying"}},
				jRow:       Jsondata{"column1": "column1datastring", "column2": "column3datastring"},
				insertCols: []string{"column1", "column3"},
				insertRows: Rows{{"previous_entry", "column3value"}},
			},
			Expected: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column1", IsNullable: "NO", DataType: "varchar"},
					DbColumn{Name: "column3", IsNullable: "YES", DataType: "character varying"}},
				jRow:       Jsondata{"column1": "column1datastring", "column2": "column3datastring", "column3": nil},
				insertCols: []string{"column1", "column3"},
				verified:   true,
				insertRows: Rows{{"previous_entry", "column3value"}, {"column1datastring", nil}},
			},
		},
		"new_row_nil_data": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column5", IsNullable: "YES", DataType: "doesn't matter"},
					DbColumn{Name: "column6", IsNullable: "YES", DataType: "doesn't matter"}},
				jRow:       Jsondata{"column1": "column1datastring"},
				insertCols: []string{"column5", "column6"},
			},
			Expected: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column5", IsNullable: "YES", DataType: "doesn't matter"},
					DbColumn{Name: "column6", IsNullable: "YES", DataType: "doesn't matter"}},
				jRow:       Jsondata{"column1": "column1datastring", "column5": nil, "column6": nil},
				insertCols: []string{"column5", "column6"},
				insertRows: Rows{{nil, nil}},
				verified:   true,
			},
		},
		"cannot_string_to_int": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column6", IsNullable: "YES", DataType: "int"},
					DbColumn{Name: "column7", IsNullable: "YES", DataType: "bigint"}},
				jRow:       Jsondata{"column6": "column6datastring", "column7": "column7datastring"},
				insertCols: []string{"column6", "column7"},
				insertRows: Rows{{nil, nil}},
			},
			ExpectedErr: errors.New("strconv.ParseInt: parsing \"column6datastring\": invalid syntax"),
		},
		"cannot_string_to_float": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column6", IsNullable: "YES", DataType: "decimal"},
					DbColumn{Name: "column7", IsNullable: "YES", DataType: "text"}},
				jRow:       Jsondata{"column6": "column6datastring", "column7": "column7datastring"},
				insertCols: []string{"column6", "column7"},
				insertRows: Rows{{nil, nil}},
			},
			ExpectedErr: errors.New("strconv.ParseFloat: parsing \"column6datastring\": invalid syntax"),
		},
		"string_to_int64": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column6", IsNullable: "YES", DataType: "bigint"},
					DbColumn{Name: "column7", IsNullable: "YES", DataType: "text"}},
				jRow:       Jsondata{"column6": "1234567890126545643", "column7": "column7datastring"},
				insertCols: []string{"column6", "column7"},
				insertRows: Rows{{nil, nil}},
			},
			Expected: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column6", IsNullable: "YES", DataType: "bigint"},
					DbColumn{Name: "column7", IsNullable: "YES", DataType: "text"}},
				jRow:       Jsondata{"column6": int64(1234567890126545643), "column7": "column7datastring"},
				insertCols: []string{"column6", "column7"},
				insertRows: Rows{{nil, nil}, {int64(1234567890126545643), "column7datastring"}},
				verified:   true,
			},
		},
		"cannot_int_float": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column6", IsNullable: "YES", DataType: "numeric"},
					DbColumn{Name: "column7", IsNullable: "YES", DataType: "bigint"}},
				jRow:       Jsondata{"column6": 12.54, "column7": 165.78},
				insertCols: []string{"column6", "column7"},
				insertRows: Rows{{11.5, 5487}},
			},
			ExpectedErr: errors.New("add_row: cannot convert number value to int64 for column7 value: 165.78 type: bigint"),
		},
		"float_to_int_should_work": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column6", IsNullable: "YES", DataType: "numeric"},
					DbColumn{Name: "column7", IsNullable: "YES", DataType: "bigint"}},
				jRow:       Jsondata{"column6": 12.54, "column7": float64(16578.0)}, // unmarshal parses all numbers as float64
				insertCols: []string{"column6", "column7"},
				insertRows: Rows{{11.5, 5487}},
			},
			Expected: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column6", IsNullable: "YES", DataType: "numeric"},
					DbColumn{Name: "column7", IsNullable: "YES", DataType: "bigint"}},
				jRow:       Jsondata{"column6": 12.54, "column7": int64(16578.0)}, // logic should convert any `int` to int64
				insertCols: []string{"column6", "column7"},
				insertRows: Rows{{11.5, 5487}, {12.54, int64(16578)}}, // row insert data should also be int64
				verified:   true,
			},
		},
	}
	trial.New(fn, cases).Test(t)
}

func TestNewWorker(t *testing.T) {
	type input struct {
		*options
		Info string
	}

	type output struct {
		Params     InfoOptions
		Invalid    bool
		Msg        string
		Count      int
		DeleteStmt string
	}

	// create a test folder and files
	f1 := "./tmp/temp1.json"
	w, _ := file.NewWriter(f1, &file.Options{})
	w.WriteLine([]byte(`{"test":"value1","testing":"value2","number":123}`))
	w.Close()

	f2 := "./tmp/temp2.json"
	w, _ = file.NewWriter(f2, &file.Options{})
	w.WriteLine([]byte(`{"test":"value1","testing":"value2","number":123}`))
	w.Close()

	d1, _ := filepath.Abs(f2)

	f3 := "./tmp1"
	os.Mkdir(f3, 0755)
	d2, _ := filepath.Abs(f3)

	fn := func(in trial.Input) (interface{}, error) {
		// set input
		i := in.Interface().(input)
		wrkr := i.options.newWorker(i.Info)
		o := output{}
		// if task is invalid set values
		o.Invalid, o.Msg = task.IsInvalidWorker(wrkr)

		// if the test isn't for a invalid worker set count and params
		if !o.Invalid {
			myw := wrkr.(*worker)
			o.Params = myw.Params
			o.Count = len(myw.flist)
			o.DeleteStmt = myw.delete
		}

		return o, nil
	}

	// testing cases
	cases := trial.Cases{
		"valid_worker": {
			Input: input{options: &options{}, Info: d1 + "?table=schema.table_name"},
			Expected: output{
				Params: InfoOptions{
					FilePath: d1,
					Table:    "schema.table_name",
				},
				Invalid: false,
				Msg:     "",
				Count:   1,
			},
		},

		"table_required": {
			Input: input{options: &options{}, Info: "nothing"},
			Expected: output{
				Invalid: true,
				Msg:     "params uri.unmarshal: table is required",
			},
		},

		"invalid_path": {
			Input: input{options: &options{}, Info: "missingfile.json?table=schema.table_name"},
			Expected: output{
				Invalid: true,
				Msg:     "filepath os: stat missingfile.json: no such file or directory",
			},
		},

		"invalid_worker": {
			Input: input{options: &options{}, Info: d2 + "?table=schema.table_name"},
			Expected: output{
				Params:  InfoOptions{},
				Invalid: true,
				Msg:     "no files found in path " + d2,
				Count:   0,
			},
		},

		"valid_path_with_delete": {
			Input: input{options: &options{}, Info: d1 + "?table=schema.table_name&delete=date:2020-07-01"},
			Expected: output{
				Params: InfoOptions{
					FilePath:  d1,
					Table:     "schema.table_name",
					DeleteMap: map[string]string{"date": "2020-07-01"},
				},
				DeleteStmt: "delete from schema.table_name where date = '2020-07-01'",
				Invalid:    false,
				Msg:        "",
				Count:      1,
			},
		},
	}

	trial.New(fn, cases).Test(t)

	// cleanup
	os.Remove(f1)
	os.Remove(f2)
	os.Remove("./tmp")
	os.Remove("./tmp1")
}
