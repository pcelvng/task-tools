package stat

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

func New() Stats {
	return Stats{}
}

// NewFromBytes creates Stats from
// json bytes.
func NewFromBytes(b []byte) Stats {
	sts := New()
	json.Unmarshal(b, &sts)
	return sts
}

type Stats struct {
	// Started is the time when the query was started.
	Started string `json:"started"`

	// Dur is the execution duration.
	Dur Duration `json:"dur"`

	// DBName
	DBName string `json:"db"`

	// DBTable is the table or schema.table value
	DBTable string `json:"table"`

	// RemovedCnt is the number of records removed before
	// the bulk insert.
	RemovedCnt int64 `json:"removed"`

	// InsertCnt is the number of records inserted with the bulk insert.
	InsertCnt int64 `json:"inserted"`

	// ColumnCnt is the number of columns of each row inserted.
	ColumnCnt int `json:"columns"`

	mu sync.Mutex
}

// AddRow will atomically increment the InsertCnt value.
func (s *Stats) AddRow() {
	atomic.AddInt64(&s.InsertCnt, 1)
}

// SetStarted will set the Created field in the
// format time.RFC3339.
func (s *Stats) SetStarted(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Started = t.Format(time.RFC3339)
}

// ParseStarted will attempt to parse the Created
// field to a time.Time object.
// ParseCreated expects the Created time string is in
// time.RFC3339. If there is a parse error
// then the time.Time zero value is returned.
func (s *Stats) ParseStarted() time.Time {
	t, _ := time.Parse(time.RFC3339, s.Started)
	return t
}

func (s Stats) JSONBytes() []byte {
	b, _ := json.Marshal(s)
	return b
}

func (s Stats) JSONString() string {
	return string(s.JSONBytes())
}

// Clone will create a copy of stat that won't trigger
// race conditions. Use Clone if you are updating and
// reading from stats at the same time. Read from the
// clone.
func (s Stats) Clone() Stats {
	return s
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(b []byte) error {
	// Ignore if there is no value set.
	if len(b) == 0 {
		return nil
	}

	// Ignore null, like in the main JSON package.
	if string(b) == "null" {
		return nil
	}
	var err error
	d.Duration, err = time.ParseDuration(string(b))
	return err
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}
