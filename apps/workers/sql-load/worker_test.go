package main

import (
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

func TestPrepareMeta(t *testing.T) {
	type input struct {
		fields map[string]string
		schema []DbColumn
	}
	type output struct {
		schema  []DbColumn
		columns []string
	}
	fn := func(in trial.Input) (interface{}, error) {
		i := in.Interface().(input)
		o := output{}
		o.schema, o.columns = PrepareMeta(i.schema, i.fields)

		// because we are dealing with a map for the jRow data
		// we need to sort the output, not required in actual processing
		sort.Strings(o.columns)

		return o, nil
	}

	// testing cases
	cases := trial.Cases{
		"no field map": { // missing keys in the json will be ignored
			Input: input{
				schema: []DbColumn{
					{Name: "C1"}, {Name: "C2"},
				},
				fields: map[string]string{},
			},
			Expected: output{
				schema: []DbColumn{
					{Name: "C1", JsonKey: "C1"},
					{Name: "C2", JsonKey: "C2"},
				},
				columns: []string{"C1", "C2"},
			},
		},
		"transpose fields": {
			Input: input{
				schema: []DbColumn{
					{Name: "C1"}, {Name: "C2"},
				},
				fields: map[string]string{"C1": "J1", "C2": "J2"},
			},
			Expected: output{
				schema: []DbColumn{
					{Name: "C1", JsonKey: "J1"},
					{Name: "C2", JsonKey: "J2"},
				},
				columns: []string{"C1", "C2"},
			},
		},
		"Partial json mapping": {
			Input: input{
				schema: []DbColumn{
					{Name: "C1"}, {Name: "C2"},
				},
				fields: map[string]string{"C1": "J1", "C3": "J2"},
			},
			Expected: output{
				schema: []DbColumn{
					{Name: "C1", JsonKey: "J1"},
					{Name: "C2", JsonKey: "C2"},
				},
				columns: []string{"C1", "C2"},
			},
		},
		"Ignore Funcs": {
			Input: input{
				schema: []DbColumn{
					{Name: "C1", Default: trial.StringP("new()")}, {Name: "C2"},
				},
				fields: map[string]string{},
			},
			Expected: output{
				schema: []DbColumn{
					{Name: "C2", JsonKey: "C2"},
				},
				columns: []string{"C2"},
			},
		},
		"Ignore -": {
			Input: input{
				schema: []DbColumn{
					{Name: "C1"}, {Name: "C2"}, {Name: "C3"},
				},
				fields: map[string]string{"C2": "-"},
			},
			Expected: output{
				schema: []DbColumn{
					{Name: "C1", JsonKey: "C1"}, {Name: "C3", JsonKey: "C3"},
				},
				columns: []string{"C1", "C3"},
			},
		},
	}

	trial.New(fn, cases).Test(t)
}

/*
func TestAddRow(t *testing.T) {
	fn := func(in trial.Input) (interface{}, error) {
		i := in.Interface().(Tester)
		err := i.ds.AddRow(i.jRow)

		// testing the json map data is super confusing as it's not 'ordered' data
		// and to test if we have the correct output it must be sorted
		sort.Strings(i.ds.insertCols)
		if len(i.ds.insertMeta) > 1 {
			sort.SliceStable(i.ds.insertMeta, func(a, b int) bool {
				if len(i.ds.insertRows) > 1 {
					// if there is output for the insertRows needs to be sorted
					// in the same order as the insertCols and insertMeta
					sort.SliceStable(i.ds.insertRows, func(a, b int) bool {
						return i.ds.insertMeta[a].Name < i.ds.insertMeta[b].Name
					})
				}
				return i.ds.insertMeta[a].Name < i.ds.insertMeta[b].Name
			})
		}

		return i, err
	}

	// testing cases
	cases := trial.Cases{
		"test_parse_float_to_int": {
			Input: Tester{
				jRow: Jsondata{"column1": float64(12345678), "column2": "687.8725"},
				ds: &DataSet{
					dbSchema: DbSchema{
						DbColumn{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"},
						DbColumn{Name: "column2", IsNullable: "YES", DataType: "numeric", TypeName: "float"}},
					insertCols: []string{"column1", "column2"},
					insertRows: Rows{{987654, 14568.57892}},
					insertMeta: []DbColumn{
						{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"},
						{Name: "column2", IsNullable: "YES", JsonKey: "column2", Nullable: true, DataType: "numeric", TypeName: "float"}},
					ignoredCols: map[string]bool{},
					fieldsMap:   map[string]string{},
				},
			},
			Expected: Tester{
				jRow: Jsondata{"column1": int64(12345678), "column2": float64(687.8725)},
				ds: &DataSet{
					dbSchema: DbSchema{
						DbColumn{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"},
						DbColumn{Name: "column2", IsNullable: "YES", DataType: "numeric", TypeName: "float"}},
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
		},

		"test_parse_sting_to_number": {
			Input: Tester{
				jRow: Jsondata{"column1": "12345678", "column2": "687.8725"},
				ds: &DataSet{
					dbSchema: DbSchema{
						DbColumn{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"},
						DbColumn{Name: "column2", IsNullable: "YES", DataType: "numeric", TypeName: "float"}},
					insertCols: []string{"column1", "column2"},
					insertRows: Rows{{987654, 14568.57892}},
					insertMeta: []DbColumn{
						{Name: "column1", IsNullable: "NO", JsonKey: "column1", DataType: "bigint", TypeName: "int"},
						{Name: "column2", IsNullable: "YES", JsonKey: "column2", Nullable: true, DataType: "numeric", TypeName: "float"}},
					ignoredCols: map[string]bool{},
					fieldsMap:   map[string]string{},
				},
			},
			Expected: Tester{
				jRow: Jsondata{"column1": int64(12345678), "column2": float64(687.8725)},
				ds: &DataSet{
					dbSchema: DbSchema{
						DbColumn{Name: "column1", IsNullable: "NO", DataType: "bigint", TypeName: "int"},
						DbColumn{Name: "column2", IsNullable: "YES", DataType: "numeric", TypeName: "float"}},
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
		},

		"adding_another_row": {
			Input: Tester{
				jRow: Jsondata{"column1": "column1datastring", "column2": "column3datastring"},
				ds: &DataSet{
					dbSchema: DbSchema{
						DbColumn{Name: "column1", IsNullable: "NO", DataType: "varchar", TypeName: "string"},
						DbColumn{Name: "column3", IsNullable: "YES", DataType: "character varying", TypeName: "string"}},
					insertCols: []string{"column1", "column3"},
					insertRows: Rows{{"previous_entry", "column3value"}},
					insertMeta: []DbColumn{
						{Name: "column1", IsNullable: "NO", JsonKey: "column1"},
						{Name: "column3", IsNullable: "YES", JsonKey: "column3", Nullable: true}},
					ignoredCols: map[string]bool{"column2": true},
					fieldsMap:   map[string]string{},
				},
			},
			Expected: Tester{
				jRow: Jsondata{"column1": "column1datastring", "column2": "column3datastring"},
				ds: &DataSet{
					dbSchema: DbSchema{
						DbColumn{Name: "column1", IsNullable: "NO", DataType: "varchar", TypeName: "string"},
						DbColumn{Name: "column3", IsNullable: "YES", DataType: "character varying", TypeName: "string"}},
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
		},

		"field_mapping_test": {
			Input: Tester{
				jRow: Jsondata{"column_1": "new_entry_1", "column_2": "new_entry_2"},
				ds: &DataSet{
					dbSchema: DbSchema{
						DbColumn{Name: "column1", IsNullable: "NO", DataType: "varchar", TypeName: "string"},
						DbColumn{Name: "column2", IsNullable: "YES", DataType: "character varying", TypeName: "string"}},
					insertCols: []string{"column1", "column2"},
					insertRows: Rows{{"previous_entry_1", "previous_entry_2"}},
					insertMeta: []DbColumn{
						{Name: "column1", IsNullable: "NO", JsonKey: "column_1"},
						{Name: "column2", IsNullable: "YES", JsonKey: "column_2", Nullable: true}},
					ignoredCols: map[string]bool{},
					fieldsMap:   map[string]string{"column_1": "column1", "column_2": "column2"}, // set from the info string
					verified:    false,
				},
			},
			Expected: Tester{
				jRow: Jsondata{"column_1": "new_entry_1", "column_2": "new_entry_2"},
				ds: &DataSet{
					dbSchema: DbSchema{
						DbColumn{Name: "column1", IsNullable: "NO", DataType: "varchar", TypeName: "string"},
						DbColumn{Name: "column2", IsNullable: "YES", DataType: "character varying", TypeName: "string"}},
					insertCols: []string{"column1", "column2"},
					insertRows: Rows{{"previous_entry_1", "previous_entry_2"}, {"new_entry_1", "new_entry_2"}},
					insertMeta: []DbColumn{
						{Name: "column1", IsNullable: "NO", JsonKey: "column_1"},
						{Name: "column2", IsNullable: "YES", JsonKey: "column_2", Nullable: true}},
					ignoredCols: map[string]bool{},
					fieldsMap:   map[string]string{"column_1": "column1", "column_2": "column2"},
					verified:    true,
				},
			},
		},
	}

	trial.New(fn, cases).Test(t)
}
*/
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
			o.DeleteStmt = myw.delQuery
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
