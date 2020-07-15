package main

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
)

func TestDefaultUpdate(t *testing.T) {

}

func TestVerifyRow(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		i := in.Interface().(*DataSet)
		err := i.VerifyRow()

		// because we are dealing with a map for the jRow data
		// we need to sort the output, not required in actual processing
		sort.Strings(i.insertCols)
		if i.insertMeta != nil {
			sort.SliceStable(i.insertMeta, func(a, b int) bool {
				return i.insertMeta[a].Name < i.insertMeta[b].Name
			})
		}

		return i, err
	}

	// testing cases
	cases := trial.Cases{
		"empty_dataset_err": {
			Input:       &DataSet{},
			ExpectedErr: errors.New("no data found in json jRow object"),
		},
		"initial_missing_key": { // missing keys in the json will be ignored
			Input: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				fieldsMap:   map[string]string{},
				ignoredCols: map[string]bool{},
			},
			Expected: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				insertCols:  []string{"column1"},
				insertMeta:  []DbColumn{{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"}},
				verified:    true,
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
			},
		},
		"missing_db_column_ignored": { // missing keys in the json will be ignored
			Input: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				fieldsMap:   map[string]string{},
				ignoredCols: map[string]bool{},
			},
			Expected: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				insertCols:  []string{"column1"},
				insertMeta:  []DbColumn{{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"}},
				verified:    true,
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
			},
		},
		"initial_all_keys": { // missing keys in the json will be ignored
			Input: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				fieldsMap:   map[string]string{},
				ignoredCols: map[string]bool{},
			},
			Expected: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				insertCols:  []string{"column1"},
				insertMeta:  []DbColumn{{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"}},
				verified:    true,
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
			},
		},
		// any fields missing from the json string are ignored, even if they are nullable
		// either the DB must handle the missing data, or the field is added in a later record
		"missing_non_nullable_field": { // missing keys in the json will be ignored
			Input: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				fieldsMap:   map[string]string{},
				ignoredCols: map[string]bool{},
			},
			Expected: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				insertCols:  []string{"column1"},
				insertMeta:  []DbColumn{{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"}},
				verified:    true,
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
			},
		},
		"insert_columns_already_set": { // missing keys in the json will be ignored
			Input: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				fieldsMap:   map[string]string{},
				ignoredCols: map[string]bool{},
			},
			Expected: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				insertCols:  []string{"column1"},
				insertMeta:  []DbColumn{{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"}},
				verified:    true,
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
			},
		},
		"all_nil_data": { // missing keys in the json will be ignored
			Input: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				fieldsMap:   map[string]string{},
				ignoredCols: map[string]bool{},
			},
			Expected: &DataSet{
				dbSchema:    DbSchema{{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"}, {Name: "column2", IsNullable: "YES"}},
				jRow:        Jsondata{"column1": "column1datastring"},
				insertCols:  []string{"column1"},
				insertMeta:  []DbColumn{{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"}},
				verified:    true,
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
			},
		},
	}

	trial.New(fn, cases).Test(t)
}

func TestAddRow(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		i := in.Interface().(*DataSet)
		err := i.AddRow()

		// testing the json map data is super confusing as it's not 'ordered' data
		// and to test if we have the correct output it must be sorted
		sort.Strings(i.insertCols)
		if len(i.insertMeta) > 1 {
			sort.SliceStable(i.insertMeta, func(a, b int) bool {
				if len(i.insertRows) > 1 {
					// if there is output for the insertRows needs to be sorted
					// in the same order as the insertCols and insertMeta
					sort.SliceStable(i.insertRows, func(a, b int) bool {
						return i.insertMeta[a].Name < i.insertMeta[b].Name
					})
				}
				return i.insertMeta[a].Name < i.insertMeta[b].Name
			})
		}

		return i, err
	}

	// testing cases
	cases := trial.Cases{
		"test_parse_float_to_int": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"},
					DbColumn{Name: "column2", IsNullable: "YES", DataType: "numeric", TypeName: "float"}},
				jRow:       Jsondata{"column1": float64(12345678), "column2": "687.8725"},
				insertCols: []string{"column1", "column2"},
				insertRows: Rows{{987654, 14568.57892}},
				insertMeta: []DbColumn{
					{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"},
					{Name: "column2", IsNullable: "YES", JsonKey: "column2", Nullable: true, DataType: "numeric", TypeName: "float"}},
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
			},
			Expected: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"},
					DbColumn{Name: "column2", IsNullable: "YES", DataType: "numeric", TypeName: "float"}},
				jRow:       Jsondata{"column1": int64(12345678), "column2": float64(687.8725)},
				insertCols: []string{"column1", "column2"},
				insertRows: Rows{{987654, 14568.57892}, {int64(12345678), float64(687.8725)}},
				insertMeta: []DbColumn{
					{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"},
					{Name: "column2", IsNullable: "YES", JsonKey: "column2", Nullable: true, DataType: "numeric", TypeName: "float"}},
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
				verified:    true,
			},
		},

		"test_parse_sting_to_number": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"},
					DbColumn{Name: "column2", IsNullable: "YES", DataType: "numeric", TypeName: "float"}},
				jRow:       Jsondata{"column1": "12345678", "column2": "687.8725"},
				insertCols: []string{"column1", "column2"},
				insertRows: Rows{{987654, 14568.57892}},
				insertMeta: []DbColumn{
					{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"},
					{Name: "column2", IsNullable: "YES", JsonKey: "column2", Nullable: true, DataType: "numeric", TypeName: "float"}},
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
			},
			Expected: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"},
					DbColumn{Name: "column2", IsNullable: "YES", DataType: "numeric", TypeName: "float"}},
				jRow:       Jsondata{"column1": int64(12345678), "column2": float64(687.8725)},
				insertCols: []string{"column1", "column2"},
				insertRows: Rows{{987654, 14568.57892}, {int64(12345678), float64(687.8725)}},
				insertMeta: []DbColumn{
					{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"},
					{Name: "column2", IsNullable: "YES", JsonKey: "column2", Nullable: true, DataType: "numeric", TypeName: "float"}},
				ignoredCols: map[string]bool{},
				fieldsMap:   map[string]string{},
				verified:    true,
			},
		},

		"adding_another_row": {
			Input: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column1", IsNullable: "NO", DataType: "varchar", TypeName: "string"},
					DbColumn{Name: "column3", IsNullable: "YES", DataType: "character varying", TypeName: "string"}},
				jRow:       Jsondata{"column1": "column1datastring", "column2": "column3datastring"},
				insertCols: []string{"column1", "column3"},
				insertRows: Rows{{"previous_entry", "column3value"}},
				insertMeta: []DbColumn{
					{Name: "column1", IsNullable: "NO", JsonKey: "column1"},
					{Name: "column3", IsNullable: "YES", JsonKey: "column3", Nullable: true}},
				ignoredCols: map[string]bool{"column2": true},
				fieldsMap:   map[string]string{},
			},
			Expected: &DataSet{
				dbSchema: DbSchema{
					DbColumn{Name: "column1", IsNullable: "NO", DataType: "varchar", TypeName: "string"},
					DbColumn{Name: "column3", IsNullable: "YES", DataType: "character varying", TypeName: "string"}},
				jRow:       Jsondata{"column1": "column1datastring", "column2": "column3datastring"},
				insertCols: []string{"column1", "column3"},
				verified:   true,
				insertRows: Rows{{"previous_entry", "column3value"}, {"column1datastring", nil}},
				insertMeta: []DbColumn{
					{Name: "column1", IsNullable: "NO", JsonKey: "column1"},
					{Name: "column3", IsNullable: "YES", JsonKey: "column3", Nullable: true}},
				ignoredCols: map[string]bool{"column2": true},
				fieldsMap:   map[string]string{},
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
		Params     InfoURI
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
				Params: InfoURI{
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
				Params:  InfoURI{},
				Invalid: true,
				Msg:     "no files found in path " + d2,
				Count:   0,
			},
		},

		"valid_path_with_delete": {
			Input: input{options: &options{}, Info: d1 + "?table=schema.table_name&delete=date(hour_utc):2020-07-09|id:1572|amt:65.2154"},
			Expected: output{
				Params: InfoURI{
					FilePath:  d1,
					Table:     "schema.table_name",
					DeleteMap: map[string]string{"date(hour_utc)": "2020-07-09", "id": "1572", "amt": "65.2154"},
				},
				DeleteStmt: "delete from schema.table_name where amt = 65.2154 and date(hour_utc) = '2020-07-09' and id = 1572",
				Invalid:    false,
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
