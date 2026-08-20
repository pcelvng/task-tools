package sqlite

import (
	"testing"

	"github.com/hydronica/trial"
	"github.com/jbsmith7741/uri"
)

func TestTaskFilter_Normalize(t *testing.T) {
	fn := func(in TaskFilter) (TaskFilter, error) {
		in.Normalize()
		return TaskFilter{Sort: in.Sort, Direction: in.Direction}, nil
	}
	cases := trial.Cases[TaskFilter, TaskFilter]{
		"empty": {
			Input:    TaskFilter{},
			Expected: TaskFilter{},
		},
		"unknown column clears": {
			Input:    TaskFilter{Sort: "bogus", Direction: "asc"},
			Expected: TaskFilter{},
		},
		"type asc": {
			Input:    TaskFilter{Sort: "type", Direction: "asc"},
			Expected: TaskFilter{Sort: "type", Direction: "asc"},
		},
		"type DESC normalized": {
			Input:    TaskFilter{Sort: "type", Direction: "DESC"},
			Expected: TaskFilter{Sort: "type", Direction: "desc"},
		},
		"invalid direction defaults asc": {
			Input:    TaskFilter{Sort: "job", Direction: "sideways"},
			Expected: TaskFilter{Sort: "job", Direction: "asc"},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestTaskFilter_orderByClause(t *testing.T) {
	fn := func(in TaskFilter) (string, error) {
		in.Normalize()
		return in.orderByClause(), nil
	}
	cases := trial.Cases[TaskFilter, string]{
		"default empty": {
			Input:    TaskFilter{},
			Expected: "ORDER BY created DESC",
		},
		"unknown column": {
			Input:    TaskFilter{Sort: "bogus", Direction: "asc"},
			Expected: "ORDER BY created DESC",
		},
		"type asc": {
			Input:    TaskFilter{Sort: "type", Direction: "asc"},
			Expected: "ORDER BY type ASC",
		},
		"type desc": {
			Input:    TaskFilter{Sort: "type", Direction: "desc"},
			Expected: "ORDER BY type DESC",
		},
		"msg": {
			Input:    TaskFilter{Sort: "msg", Direction: "asc"},
			Expected: "ORDER BY msg ASC",
		},
		"queue_seconds": {
			Input:    TaskFilter{Sort: "queue_seconds", Direction: "desc"},
			Expected: "ORDER BY queue_seconds DESC",
		},
		"task_seconds": {
			Input:    TaskFilter{Sort: "task_seconds", Direction: "asc"},
			Expected: "ORDER BY task_seconds ASC",
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestTaskFilter_QueryString(t *testing.T) {
	fn := func(in *TaskFilter) (string, error) {
		return in.QueryString(), nil
	}
	cases := trial.Cases[*TaskFilter, string]{
		"nil": {
			Input:    nil,
			Expected: "",
		},
		"empty": {
			Input:    &TaskFilter{},
			Expected: "",
		},
		"multi filters and sort": {
			Input: &TaskFilter{
				ID:        []string{"a", "b"},
				Type:      []string{"alpha"},
				Job:       []string{"load", "check"},
				Result:    []string{"error", "running"},
				Sort:      "created",
				Direction: "desc",
			},
			Expected: "direction=desc&id=a%2Cb&job=load%2Ccheck&result=error%2Crunning&sort=created&type=alpha",
		},
		"omits page and limit": {
			Input:    &TaskFilter{Type: []string{"x"}, Page: 3, Limit: 50},
			Expected: "type=x",
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestTaskFilter_URIUnmarshal(t *testing.T) {
	fn := func(raw string) (TaskFilter, error) {
		f := TaskFilter{}
		if err := uri.UnmarshalQuery(raw, &f); err != nil {
			return TaskFilter{}, err
		}
		f.Normalize()
		return f, nil
	}
	cases := trial.Cases[string, TaskFilter]{
		"comma separated": {
			Input: "type=alpha,zebra&result=error,alert&id=abc,def",
			Expected: TaskFilter{
				ID:     []string{"abc", "def"},
				Type:   []string{"alpha", "zebra"},
				Result: []string{"error", "alert"},
			},
		},
		"repeated params": {
			Input: "type=alpha&type=zebra&job=import",
			Expected: TaskFilter{
				Type: []string{"alpha", "zebra"},
				Job:  []string{"import"},
			},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestWhereBuilder(t *testing.T) {
	type input struct {
		date   string
		id     []string
		typ    []string
		result []string
	}
	type output struct {
		SQL  string
		Args []any
	}

	fn := func(in input) (output, error) {
		w := &whereBuilder{}
		if in.date != "" {
			w.And("DATE(created) = ?", in.date)
		}
		w.In("id", in.id)
		w.In("type", in.typ)
		w.Result(in.result)
		sql, args := w.SQL()
		return output{SQL: sql, Args: args}, nil
	}
	cases := trial.Cases[input, output]{
		"date only": {
			Input:    input{date: "2024-01-15"},
			Expected: output{SQL: "WHERE DATE(created) = ?", Args: []any{"2024-01-15"}},
		},
		"in clauses": {
			Input: input{
				date: "2024-01-15",
				id:   []string{"a", "b"},
				typ:  []string{"alpha"},
			},
			Expected: output{
				SQL:  "WHERE DATE(created) = ? AND id IN (?,?) AND type IN (?)",
				Args: []any{"2024-01-15", "a", "b", "alpha"},
			},
		},
		"result running and error": {
			Input: input{
				date:   "2024-01-15",
				result: []string{"running", "error"},
			},
			Expected: output{
				SQL:  "WHERE DATE(created) = ? AND (result = '' OR result IN (?))",
				Args: []any{"2024-01-15", "error"},
			},
		},
		"empty": {
			Input:    input{},
			Expected: output{},
		},
	}
	trial.New(fn, cases).SubTest(t)
}
