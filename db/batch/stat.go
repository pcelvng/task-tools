package batch

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

func NewStats() Stats {
	return Stats{}
}

// NewFromBytes creates Stats from
// json bytes.
func NewStatsFromBytes(b []byte) Stats {
	sts := NewStats()
	json.Unmarshal(b, &sts)
	return sts
}

type Stats struct {
	// Started is the time when the query was started.
	Started string `json:"started"`

	// Dur is the execution duration.
	Dur Duration `json:"dur"`

	// Table is the table or schema.table value
	Table string `json:"table"`

	// Removed is the number of records removed before
	// the bulk insert.
	Removed int64 `json:"removed"`

	// Rows is the number of raw rows added. This is not
	// the actual insert numbers reported back by the db
	// after inserting but should be.
	Rows int64 `json:"rows"`

	// Inserted is the number of records inserted with the bulk insert.
	// This is the actual number reported back by the db.
	Inserted int64 `json:"inserted"`

	// Cols is the number of columns of each row inserted.
	Cols int `json:"cols"`

	// BatchDate is the hour of data for which the
	// batch data belongs. Not populated by bulk
	// inserter.
	BatchDate string `json:"batch_hour"`

	mu sync.Mutex
}

// AddRow will atomically increment the Inserted value.
func (s *Stats) AddRow() {
	atomic.AddInt64(&s.Inserted, 1)
}

// SetStarted will set the Created field in the
// format time.RFC3339.
func (s *Stats) SetStarted(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Started = t.Format(time.RFC3339)
}

// SetBatchDate will set the Created field in the
// format time.RFC3339.
func (s *Stats) SetBatchDate(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.BatchDate = t.Format(time.RFC3339)
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

// ParseBatchDate will attempt to parse the Created
// field to a time.Time object.
// ParseCreated expects the Created time string is in
// time.RFC3339. If there is a parse error
// then the time.Time zero value is returned.
func (s *Stats) ParseBatchDate() time.Time {
	t, _ := time.Parse(time.RFC3339, s.BatchDate)
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
