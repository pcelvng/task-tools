package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/mock"
)

func TestDeleteCustom(t *testing.T) {
	type input struct {
		table   string
		encoded string
	}
	fn := func(in input) (string, error) {
		out := CustomDelete(in.encoded, in.table)
		return out, nil
	}
	cases := trial.Cases[input, string]{
		"test1": {
			Input: input{
				table:   "mytable",
				encoded: `hour_local%20%3E%3D%20%272023-01-02T00%3A00%3A00%27%20and%20hour_local%20%3C%3D%20%272023-01-02T23%3A00%3A00%27%20and%20account_id%20%3D%20560277441796101`,
			},
			Expected: `delete from mytable where hour_local >= '2023-01-02T00:00:00' and hour_local <= '2023-01-02T23:00:00' and account_id = 560277441796101`,
		},
	}

	trial.New(fn, cases).Test(t)
}

func TestDefaultUpdate(t *testing.T) {

}

func TestPrepareMeta(t *testing.T) {
	type input struct {
		fields map[string]string
		meta   *TableMeta
	}

	fn := func(in input) (*TableMeta, error) {
		in.meta.PrepareMeta(in.fields)

		// because we are dealing with a map for the jRow data
		// we need to sort the output, not required in actual processing
		sort.Strings(in.meta.colNames)

		return in.meta, nil
	}

	// testing cases
	cases := trial.Cases[input, *TableMeta]{
		"no field map": { // missing keys in the json will be ignored
			Input: input{
				meta: &TableMeta{
					dbSchema: []DbColumn{
						{Name: "C1"}, {Name: "C2"},
					},
				},
				fields: map[string]string{},
			},
			Expected: &TableMeta{
				dbSchema: []DbColumn{
					{Name: "C1", FieldKey: "C1"},
					{Name: "C2", FieldKey: "C2"},
				},
				colNames: []string{"C1", "C2"},
				colTypes: []string{"", ""},
			},
		},
		"transpose fields": {
			Input: input{
				meta: &TableMeta{
					dbSchema: []DbColumn{
						{Name: "C1"}, {Name: "C2"},
					},
				},
				fields: map[string]string{"C1": "J1", "C2": "J2"},
			},
			Expected: &TableMeta{
				dbSchema: []DbColumn{
					{Name: "C1", FieldKey: "J1", Default: trial.StringP("")},
					{Name: "C2", FieldKey: "J2", Default: trial.StringP("")},
				},
				colNames: []string{"C1", "C2"},
				colTypes: []string{"", ""},
			},
		},
		"Partial json mapping": {
			Input: input{
				meta: &TableMeta{
					dbSchema: []DbColumn{
						{Name: "C1", Nullable: true}, {Name: "C2", Nullable: true},
					},
				},
				fields: map[string]string{"C1": "J1", "C3": "J2"},
			},
			Expected: &TableMeta{
				dbSchema: []DbColumn{
					{Name: "C1", FieldKey: "J1", Nullable: true},
					{Name: "C2", FieldKey: "C2", Nullable: true},
				},
				colNames: []string{"C1", "C2"},
				colTypes: []string{"", ""},
			},
		},
		"Ignore Funcs": {
			Input: input{
				meta: &TableMeta{
					dbSchema: []DbColumn{
						{Name: "C1", Default: trial.StringP("new()")}, {Name: "C2"},
					},
				},
				fields: map[string]string{},
			},
			Expected: &TableMeta{
				dbSchema: []DbColumn{
					{Name: "C2", FieldKey: "C2"},
				},
				colNames: []string{"C2"},
				colTypes: []string{""},
			},
		},
		"Ignore -": {
			Input: input{
				meta: &TableMeta{
					dbSchema: []DbColumn{
						{Name: "C1"}, {Name: "C2"}, {Name: "C3"},
					},
				},
				fields: map[string]string{"C2": "-"},
			},
			Expected: &TableMeta{
				dbSchema: []DbColumn{
					{Name: "C1", FieldKey: "C1"}, {Name: "C3", FieldKey: "C3"},
				},
				colNames: []string{"C1", "C3"},
				colTypes: []string{"", ""},
			},
		},
		"add defaults when in fieldMap": {
			Input: input{
				meta: &TableMeta{
					dbSchema: []DbColumn{
						{Name: "id", Nullable: false, TypeName: "int"},
						{Name: "name", Nullable: false, TypeName: "string"},
						{Name: "value", Nullable: false, TypeName: "float"},
					},
				},
				fields: map[string]string{"id": "json_id", "name": "jName", "value": "jvalue"},
			},
			Expected: &TableMeta{
				dbSchema: []DbColumn{
					{Name: "id", FieldKey: "json_id", Default: trial.StringP("0"), Nullable: false, TypeName: "int"},
					{Name: "name", FieldKey: "jName", Default: trial.StringP(""), Nullable: false, TypeName: "string"},
					{Name: "value", FieldKey: "jvalue", Default: trial.StringP("0.0"), Nullable: false, TypeName: "float"},
				},
				colNames: []string{"id", "name", "value"},
				colTypes: []string{"int", "string", "float"},
			},
		},
	}

	trial.New(fn, cases).Test(t)
}

func TestMakeCsvHeader(t *testing.T) {
	fn := func(in []byte) ([]string, error) {
		return MakeCsvHeader(in, []rune(",")[0])
	}
	cases := trial.Cases[[]byte, []string]{
		"strings_header": {
			Input:    []byte(`"first","second","third","fourth"`),
			Expected: []string{"first", "second", "third", "fourth"},
		},
		"just_values_header": {
			Input:    []byte(`first,second,third,fourth`),
			Expected: []string{"first", "second", "third", "fourth"},
		},
		"mixed_values_header": {
			Input:    []byte(`"first",second,"third",fourth`),
			Expected: []string{"first", "second", "third", "fourth"},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestMakeCsvRow(t *testing.T) {
	schema := []DbColumn{
		{Name: "id", FieldKey: "id"},
		{Name: "name", FieldKey: "name", Nullable: true},
		{Name: "count", FieldKey: "count", TypeName: "int", Default: trial.StringP("0")},
		{Name: "percent", FieldKey: "percent", TypeName: "float", Nullable: true},
		{Name: "num", FieldKey: "num", TypeName: "int", Nullable: true},
	}

	fn := func(in string) (Row, error) {
		header := []string{"id", "name", "count", "percent", "num"}
		return MakeCsvRow(schema, []byte(in), header, []rune(",")[0])
	}
	cases := trial.Cases[string, Row]{
		"test_csv": {
			Input:    `"av1","myname",654321,0.145,123`,
			Expected: Row{"av1", "myname", int64(654321), 0.145, int64(123)},
		},
		"strings_csv": {
			Input:    `"av1","myname","654321","0.145","123"`,
			Expected: Row{"av1", "myname", int64(654321), 0.145, int64(123)},
		},
		"float_to_int_csv": {
			Input:    `"av1","myname","654321","0.145","123.01"`,
			Expected: Row{"av1", "myname", int64(654321), 0.145, int64(123)},
		},
	}

	trial.New(fn, cases).SubTest(t)
}

func TestMakeRow(t *testing.T) {
	// testing struct for loading json data
	type jsonStruct struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	schema := []DbColumn{
		{Name: "id", FieldKey: "id"},
		{Name: "name", FieldKey: "name", Nullable: true},
		{Name: "count", FieldKey: "count", TypeName: "int", Default: trial.StringP("0")},
		{Name: "percent", FieldKey: "percent", TypeName: "float", Nullable: true},
		{Name: "num", FieldKey: "num", TypeName: "int", Nullable: true},
		{Name: "json_field", FieldKey: "json_field", TypeName: "json", Nullable: true},
	}
	fn := func(in map[string]any) (Row, error) {
		return MakeRow(schema, in)
	}
	cases := trial.Cases[map[string]any, Row]{
		"full row": {
			Input: map[string]interface{}{
				"id":         "1234",
				"name":       "apple",
				"count":      10,
				"percent":    0.24,
				"num":        2,
				"json_field": jsonStruct{ID: 1234, Name: "json_test_full_row"},
			},
			Expected: Row{"1234", "apple", 10, 0.24, 2, jsonStruct{ID: 1234, Name: "json_test_full_row"}},
		},
		"strings": {
			Input: map[string]interface{}{
				"id":         "1234",
				"name":       "apple",
				"count":      "10",
				"percent":    "0.24",
				"num":        "2",
				"json_field": `{"id":1234,"name":"json_test_strings"}`,
			},
			Expected: Row{"1234", "apple", int64(10), 0.24, int64(2), `{"id":1234,"name":"json_test_strings"}`},
		},
		"float to int": {
			Input: map[string]interface{}{
				"id":      "1234",
				"name":    "apple",
				"count":   10,
				"percent": 0.24,
				"num":     2.00,
			},
			Expected: Row{"1234", "apple", 10, 0.24, int64(2), nil},
		},
		"nulls": {
			Input: map[string]interface{}{
				"id":    "1234",
				"count": "10",
			},
			Expected: Row{"1234", nil, int64(10), nil, nil, nil},
		},
		"missing required": {
			Input:       map[string]interface{}{},
			ExpectedErr: errors.New("id is required"),
		},
		"truncate float": {
			Input: map[string]interface{}{
				"id":  "1234",
				"num": 2.4,
			},
			ExpectedErr: errors.New("cannot convert number"),
		},
		"defaults": {
			Input: map[string]interface{}{
				"id": "1234",
			},
			Expected: Row{"1234", nil, "0", nil, nil, nil},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestNewWorker(t *testing.T) {
	type input struct {
		*options
		Info string
	}

	type output struct {
		Params     InfoURI
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

	s, _ := filepath.Abs(f2)
	d1, _ := filepath.Split(s)
	d1 += "*"

	f3 := "./tmp1"
	os.Mkdir(f3, 0755)
	d2, _ := filepath.Abs(f3)

	fn := func(in input) (output, error) {
		// set input

		wrkr := in.options.newWorker(in.Info)
		o := output{}
		// if task is invalid set values
		if invalid, msg := task.IsInvalidWorker(wrkr); invalid {
			return o, errors.New(msg)
		}

		// if the test isn't for a invalid worker set count and params
		myw := wrkr.(*worker)
		o.Params = myw.Params
		if myw.fReader != nil {
			o.Count = int(myw.fReader.Stats().Files)
		}
		o.DeleteStmt = myw.delQuery

		return o, nil
	}
	// testing cases
	cases := trial.Cases[input, output]{
		"valid_worker": {
			Input: input{options: &options{}, Info: d1 + "?table=schema.table_name"},
			Expected: output{
				Params: InfoURI{
					FilePath:  d1,
					FileType:  "json",
					Table:     "schema.table_name",
					BatchSize: 10000,
					Delimiter: ",",
				},
				Count: 2,
			},
		},

		"table_required": {
			Input:       input{options: &options{}, Info: "nothing"},
			ExpectedErr: errors.New("params uri.unmarshal: table is required"),
		},

		"invalid_path": {
			Input:       input{options: &options{}, Info: "missingfile.json?table=schema.table_name"},
			ExpectedErr: errors.New("no files found for missingfile.json"),
		},

		"invalid_worker": {
			Input:       input{options: &options{}, Info: d2 + "?table=schema.table_name"},
			ExpectedErr: errors.New("no files found for " + d2),
		},

		"valid_path_with_delete": {
			Input: input{options: &options{}, Info: d1 + "?table=schema.table_name&delete=date(hour_utc):2020-07-09|id:1572|amt:65.2154"},
			Expected: output{
				Params: InfoURI{
					FilePath:  d1,
					FileType:  "json",
					Table:     "schema.table_name",
					BatchSize: 10000,
					DeleteMap: map[string]string{"date(hour_utc)": "2020-07-09", "id": "1572", "amt": "65.2154"},
					Delimiter: ",",
				},
				DeleteStmt: "delete from schema.table_name where amt = 65.2154 and date(hour_utc) = '2020-07-09' and id = 1572",
				Count:      2,
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

func TestCreateInserts(t *testing.T) {
	type input struct {
		table     string
		columns   []string
		rows      []Row
		batchSize int
		colTypes  []string
	}
	fn := func(in input) ([]string, error) {
		inChan := make(chan Row)
		outChan := make(chan string)
		doneChan := make(chan struct{})
		go func() {
			for _, r := range in.rows {
				inChan <- r
			}
			close(inChan)
		}()
		result := make([]string, 0)
		go func() {
			for s := range outChan {
				result = append(result, s)
			}
			close(doneChan)
		}()
		if len(in.colTypes) == 0 {
			in.colTypes = make([]string, len(in.columns))
		}
		CreateInserts(inChan, outChan, in.table, in.columns, in.colTypes, in.batchSize)
		<-doneChan
		return result, nil
	}

	cases := trial.Cases[input, []string]{
		"json_object": {
			Input: input{
				table:   "test",
				columns: []string{"json_column"},
				rows: []Row{
					{map[string]any{"array": []string{"a", "b", "c"}, "one": 1, "string": "string"}}, // insert data
				},
				batchSize: 10,
				colTypes:  []string{"json"}, // data type
			},
			Expected: []string{
				"insert into test(json_column)\n  VALUES \n('" + `{"array":["a","b","c"],"one":1,"string":"string"}` + "');\n",
			},
		},
		"json_array": {
			Input: input{
				table:     "test",
				columns:   []string{"json_column"},
				rows:      []Row{{[]string{"a", "b", "c"}}},
				batchSize: 10,
				colTypes:  []string{"array"},
			},
			Expected: []string{
				"insert into test(json_column)\n  VALUES \n('{\"a\",\"b\",\"c\"}');\n",
			},
		},
		"basic": {
			Input: input{
				table:     "test",
				columns:   []string{"ab", "cd", "ef"},
				rows:      []Row{{1, 2, 3}},
				batchSize: 1,
			},
			Expected: []string{"insert into test(ab,cd,ef)\n  VALUES \n(1,2,3);\n"},
		},
		"2_batches": {
			Input: input{
				table:     "test",
				columns:   []string{"ab", "cd", "ef"},
				rows:      []Row{{1, 2, 3}, {1, 2, 3}},
				batchSize: 1,
			},
			Expected: []string{
				"insert into test(ab,cd,ef)\n  VALUES \n(1,2,3);\n",
				"insert into test(ab,cd,ef)\n  VALUES \n(1,2,3);\n",
			},
		},
		"4_records": {
			Input: input{
				table:     "test",
				columns:   []string{"ab", "cd", "ef"},
				rows:      []Row{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11, 12}},
				batchSize: 10,
			},
			Expected: []string{
				"insert into test(ab,cd,ef)\n  VALUES \n(1,2,3),\n(4,5,6),\n(7,8,9),\n(10,11,12);\n",
			},
		},
		"conversion": {
			Input: input{
				table:     "test",
				columns:   []string{"a", "b", "c"},
				rows:      []Row{{int64(1), "2", 3.2}, {true, false, nil}},
				batchSize: 10,
			},
			Expected: []string{
				"insert into test(a,b,c)\n  VALUES \n(1,'2',3.2),\n(true,false,NULL);\n",
			},
		},
		"array_string": {
			Input: input{
				table:     "test",
				columns:   []string{"string_array"},
				rows:      []Row{{[]any{"a", "b", "c"}}},
				batchSize: 10,
				colTypes:  []string{"json"},
			},
			Expected: []string{
				"insert into test(string_array)\n  VALUES \n('[\"a\",\"b\",\"c\"]');\n",
			},
		},
		"array_column_string": {
			Input: input{
				table:     "test",
				columns:   []string{"string_array"},
				rows:      []Row{{[]any{"a", "b", "c"}}},
				batchSize: 10,
				colTypes:  []string{"array"},
			},
			Expected: []string{
				"insert into test(string_array)\n  VALUES \n('{\"a\",\"b\",\"c\"}');\n",
			},
		},
		"array_quote_string": {
			Input: input{
				table:    "test",
				columns:  []string{"string_array"},
				rows:     []Row{{[]any{"a'b", "cd", "ef"}}},
				colTypes: []string{"array"},
			},
			Expected: []string{"insert into test(string_array)\n  VALUES \n('{\"a''b\",\"cd\",\"ef\"}');\n"},
		},
		"array_int": {
			Input: input{
				table:     "test",
				columns:   []string{"int_array"},
				colTypes:  []string{"array"},
				rows:      []Row{{[]any{1, 2, 3}}},
				batchSize: 10,
			},
			Expected: []string{
				"insert into test(int_array)\n  VALUES \n('{1,2,3}');\n",
			},
		},
		"array_int64": {
			Input: input{
				table:     "test",
				columns:   []string{"int_array"},
				rows:      []Row{{[]any{int64(6), int64(7), int64(8)}}},
				batchSize: 10,
			},
			Expected: []string{
				"insert into test(int_array)\n  VALUES \n('{6,7,8}');\n",
			},
		},
		"array_float64": {
			Input: input{
				table:     "test",
				columns:   []string{"float_array"},
				rows:      []Row{{[]any{2.71828, 3.14159, 1.61803}}},
				batchSize: 10,
				colTypes:  []string{"array"},
			},
			Expected: []string{
				"insert into test(float_array)\n  VALUES \n('{2.71828,3.14159,1.61803}');\n",
			},
		},
		"internal": {
			Input: input{
				table:     "test",
				columns:   []string{"interval"},
				rows:      []Row{{10 * time.Second}},
				batchSize: 10,
			},
			Expected: []string{
				"insert into test(interval)\n  VALUES \n('10s');\n",
			},
		},
	}

	trial.New(fn, cases).SubTest(t)
}

func TestReadFiles(t *testing.T) { // flaky test
	c := trial.CaptureLog()
	defer c.ReadAll()

	type input struct {
		lines      []string
		skipErrors bool
	}
	type out struct {
		rowCount  int32
		skipCount int
	}
	fn := func(in input) (out, error) {
		ds := TableMeta{
			dbSchema: []DbColumn{
				{Name: "id", FieldKey: "id"},
				{Name: "name", FieldKey: "name", Nullable: true},
				{Name: "count", FieldKey: "count", TypeName: "int", Nullable: true}},
		}
		reader := mock.NewReader("nop").AddLines(in.lines...)

		rowChan := make(chan Row)
		doneChan := make(chan struct{})
		go func() {
			for range rowChan {
			}
			close(doneChan)
		}()
		ds.ReadFiles(context.Background(), reader, rowChan, in.skipErrors)
		<-doneChan
		time.Sleep(time.Millisecond * 5) // still getting race conditions where the test ends before the go routine

		return out{rowCount: ds.rowCount, skipCount: ds.skipCount}, ds.err // number of rows or error
	}
	cases := trial.Cases[input, out]{
		"valid data": {
			Input: input{
				lines: []string{
					`{"id":1, "name":"apple", "count":10}`,
					`{"id":1, "name":"banana", "count":3}`,
				},
			},
			Expected: out{rowCount: 2},
		},
		"invalid row": {
			Input: input{
				lines: []string{
					`"id":1, "name":"apple", "count":10}`,
					`{"id":1, "name":"banana", "count":3}`,
				},
			},
			ShouldErr: true,
		},
		"all invalid": {
			Input: input{
				lines: []string{
					`{`, `{`, `{`, `{`, `{`, `{`, `{`, `{`, `{`, `{`,
					`{`, `{`, `{`, `{`, `{`, `{`, `{`, `{`, `{`, `{`,
					`{`, `{`, `{`, `{`, `{`, `{`, `{`, `{`, `{`, `{`,
				},
			},
			ShouldErr: true,
		},
		"skip_invalids": {
			Input: input{
				lines: []string{
					`{`, `{`, `{`, `{`, `{`,
					`{"id":1, "name":"apple", "count":10}`,
					`{"id":1, "name":"banana", "count":3}`,
					`{"id":1, "name":"apple", "count":10}`,
					`{"id":1, "name":"banana", "count":3}`,
				},
				skipErrors: true,
			},
			Expected: out{skipCount: 5, rowCount: 4},
		},
	}
	trial.New(fn, cases).Timeout(5 * time.Second).SubTest(t)
}

func TestCSVReadFiles(t *testing.T) {
	c := trial.CaptureLog()
	defer c.ReadAll()

	type input struct {
		lines      []string
		delimiter  string
		skipErrors bool
	}
	type out struct {
		rowCount  int32
		skipCount int
	}
	fn := func(in input) (out, error) {

		if in.delimiter == "" {
			in.delimiter = ","
		}
		ds := TableMeta{
			csv:       true,
			delimiter: []rune(in.delimiter)[0],
			dbSchema: []DbColumn{
				{Name: "id", FieldKey: "id"},
				{Name: "name", FieldKey: "name", Nullable: true},
				{Name: "count", FieldKey: "count", TypeName: "int", Nullable: true}},
		}

		reader := mock.NewReader("nop").AddLines(in.lines...)

		rowChan := make(chan Row)
		doneChan := make(chan struct{})
		// consume and discard output data
		go func() {
			for range rowChan {
			}
			close(doneChan)
		}()
		ds.ReadFiles(context.Background(), reader, rowChan, in.skipErrors)
		<-doneChan
		return out{rowCount: ds.rowCount, skipCount: ds.skipCount}, ds.err // number of rows or error
	}
	cases := trial.Cases[input, out]{
		"invalid row": {
			Input: input{
				lines: []string{
					`"id","name","count"`,
					`"1a","banana",3`,
					`2b,appl"e,4`,
				},
			},
			ShouldErr: true,
		},
		"valid_csv_data": {
			Input: input{
				lines: []string{
					`"id","name","count"`,
					`"1a","banana","3"`,
					`"id","name","count"`, // simulate another file with a header row
					`2b,apple,4`,
				},
			},
			Expected: out{rowCount: 2},
		},
		"tab_delimited": {
			Input: input{
				delimiter: "\t",
				lines: []string{
					`"id"` + "\t" + `"name"` + "\t" + `"count"`,
					`"1a"` + "\t" + `"banana"` + "\t" + `"3"`,
					`"id"` + "\t" + `"name"` + "\t" + `"count"`, // simulate another file with a header row
					`2b` + "\t" + `apple` + "\t" + `4`,
				},
			},
			Expected: out{rowCount: 2},
		},
	}
	trial.New(fn, cases).Timeout(6 * time.Second).SubTest(t)
}
