package main

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

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

func TestMakeRow(t *testing.T) {
	schema := []DbColumn{
		{Name: "id", JsonKey: "id"},
		{Name: "name", JsonKey: "name", Nullable: true},
		{Name: "count", JsonKey: "count", TypeName: "int", Nullable: true},
		{Name: "percent", JsonKey: "percent", TypeName: "float", Nullable: true},
		{Name: "num", JsonKey: "num", TypeName: "int", Nullable: true},
	}
	fn := func(in trial.Input) (interface{}, error) {
		return MakeRow(schema, in.Interface().(map[string]interface{}))
	}
	cases := trial.Cases{
		"full row": {
			Input: map[string]interface{}{
				"id":      "1234",
				"name":    "apple",
				"count":   10,
				"percent": 0.24,
				"num":     2,
			},
			Expected: Row{"1234", "apple", 10, 0.24, 2},
		},
		"strings": {
			Input: map[string]interface{}{
				"id":      "1234",
				"name":    "apple",
				"count":   "10",
				"percent": "0.24",
				"num":     "2",
			},
			Expected: Row{"1234", "apple", int64(10), 0.24, int64(2)},
		},
		"float to int": {
			Input: map[string]interface{}{
				"id":      "1234",
				"name":    "apple",
				"count":   10,
				"percent": 0.24,
				"num":     2.00,
			},
			Expected: Row{"1234", "apple", 10, 0.24, int64(2)},
		},
		"nulls": {
			Input: map[string]interface{}{
				"id": "1234",
			},
			Expected: Row{"1234", nil, nil, nil, nil},
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
					FilePath:  d1,
					Table:     "schema.table_name",
					BatchSize: 1000,
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
					BatchSize: 1000,
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

func TestCreateInserts(t *testing.T) {
	type input struct {
		table     string
		columns   []string
		rows      []Row
		batchSize int
	}
	fn := func(in trial.Input) (interface{}, error) {
		inChan := make(chan Row)
		outChan := make(chan string)
		doneChan := make(chan struct{})
		i := in.Interface().(input)
		go func() {
			for _, r := range i.rows {
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
		CreateInserts(inChan, outChan, i.table, i.columns, i.batchSize)
		<-doneChan
		return result, nil
	}

	cases := trial.Cases{
		"basic": {
			Input: input{
				table:     "test",
				columns:   []string{"ab", "cd", "ef"},
				rows:      []Row{Row{1, 2, 3}},
				batchSize: 1,
			},
			Expected: []string{"insert into test(ab,cd,ef)\n  VALUES \n(1,2,3);\n"},
		},
		"2 batches": {
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
		"4 lines": {
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
	}
	trial.New(fn, cases).Timeout(5 * time.Second).SubTest(t)
}

func TestReadFiles(t *testing.T) {

}
