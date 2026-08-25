package sqlite

import (
	"net/url"
	"strings"
	"time"
)

// Task result values stored in task_records.result and used in filters.
const (
	ResultComplete = "complete"
	ResultError    = "error"
	ResultAlert    = "alert"
	ResultWarn     = "warn"
	ResultRunning  = "running"
)

// TaskFilter contains options for filtering and paginating task queries.
// Empty / nil slice fields are ignored in the query.
// Type, Job, and Result support multi-select via comma-separated or repeated query params
// (parsed with github.com/jbsmith7741/uri).
type TaskFilter struct {
	ID        []string `uri:"id"`
	Type      []string `uri:"type"`
	Job       []string `uri:"job"`
	Result    []string `uri:"result"` // complete, error, alert, warn, or running
	Sort      string   `uri:"sort"`
	Direction string   `uri:"direction"` // "asc" or "desc"
	Page      int      `uri:"page"`      // 1-based, default: 1
	Limit     int      `uri:"-"`         // not taken from the URL
}

// taskSortColumns is the set of safe column names allowed in ORDER BY.
var taskSortColumns = map[string]bool{
	"id": true, "type": true, "job": true, "msg": true, "result": true,
	"info": true, "meta": true, "created": true,
	"queue_seconds": true, "task_seconds": true,
}

// QueryString encodes filter/sort params for pagination links (no date/page).
// Multi-value fields use comma-separated form.
func (f *TaskFilter) QueryString() string {
	if f == nil {
		return ""
	}
	q := url.Values{}
	if len(f.ID) > 0 {
		q.Set("id", strings.Join(f.ID, ","))
	}
	if len(f.Type) > 0 {
		q.Set("type", strings.Join(f.Type, ","))
	}
	if len(f.Job) > 0 {
		q.Set("job", strings.Join(f.Job, ","))
	}
	if len(f.Result) > 0 {
		q.Set("result", strings.Join(f.Result, ","))
	}
	if f.Sort != "" {
		q.Set("sort", f.Sort)
		if f.Direction != "" {
			q.Set("direction", f.Direction)
		}
	}
	return q.Encode()
}

// Normalize validates Sort/Direction against the whitelist.
// Unknown sort keys are cleared (query uses default created DESC).
// Direction is normalized to "asc" or "desc".
func (f *TaskFilter) Normalize() {
	if f == nil {
		return
	}
	if !taskSortColumns[f.Sort] {
		f.Sort = ""
		f.Direction = ""
		return
	}
	if strings.EqualFold(f.Direction, "desc") {
		f.Direction = "desc"
	} else {
		f.Direction = "asc"
	}
}

// orderByClause returns a safe ORDER BY clause from the filter sort fields.
// Unknown columns fall back to created DESC (the historical default).
func (f *TaskFilter) orderByClause() string {
	if f == nil || !taskSortColumns[f.Sort] {
		return "ORDER BY created DESC"
	}
	dir := "ASC"
	if f.Direction == "desc" {
		dir = "DESC"
	}
	return "ORDER BY " + f.Sort + " " + dir
}

// whereForDate builds a WHERE clause for tasks on a given date with optional ID/type/job/result filters.
func (f *TaskFilter) whereForDate(date time.Time) (string, []any) {
	if f == nil {
		f = &TaskFilter{}
	}
	w := &whereBuilder{}
	w.And("DATE(created) = ?", date.Format("2006-01-02"))
	w.In("id", f.ID)
	w.In("type", f.Type)
	w.In("job", f.Job)
	w.Result(f.Result)
	return w.SQL()
}

func sliceContains(vals []string, want string) bool {
	for _, v := range vals {
		if v == want {
			return true
		}
	}
	return false
}

// whereBuilder accumulates AND clauses and bound args for SQL WHERE building.
type whereBuilder struct {
	clauses []string
	args    []any
}

func (w *whereBuilder) And(sql string, args ...any) {
	w.clauses = append(w.clauses, sql)
	w.args = append(w.args, args...)
}

// In adds column IN (?,?,...) for non-empty values.
func (w *whereBuilder) In(column string, values []string) {
	if len(values) == 0 {
		return
	}
	placeholders := make([]string, len(values))
	for i, v := range values {
		placeholders[i] = "?"
		w.args = append(w.args, v)
	}
	w.clauses = append(w.clauses, column+" IN ("+strings.Join(placeholders, ",")+")")
}

// Result adds a multi-select result filter.
func (w *whereBuilder) Result(results []string) {
	w.In("result", results)
}

func (w *whereBuilder) SQL() (string, []any) {
	if len(w.clauses) == 0 {
		return "", w.args
	}
	return "WHERE " + strings.Join(w.clauses, " AND "), w.args
}
