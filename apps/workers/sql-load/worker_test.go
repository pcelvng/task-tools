package main

import (
	"context"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task-tools/file/mock"
)

func TestReadFiles(t *testing.T) {
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
	fn := func(in trial.Input) (interface{}, error) {
		i := in.Interface().(input)
		ds := DataSet{
			dbSchema: []DbColumn{
				{Name: "id", JsonKey: "id"},
				{Name: "name", JsonKey: "name", Nullable: true},
				{Name: "count", JsonKey: "count", TypeName: "int", Nullable: true}},
		}
		reader := mock.NewReader("nop").AddLines(i.lines...)

		rowChan := make(chan Row)
		doneChan := make(chan struct{})
		go func() {
			for range rowChan {
			}
			close(doneChan)
		}()
		ds.ReadFiles(context.Background(), reader, rowChan, i.skipErrors)
		<-doneChan
		return out{rowCount: ds.rowCount, skipCount: ds.skipCount}, ds.err // number of rows or error
	}
	cases := trial.Cases{
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
		"skip invalids": {
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
				rows:      []Row{{1, 2, 3}},
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
