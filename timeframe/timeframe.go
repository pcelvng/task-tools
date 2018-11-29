package timeframe

import (
	"fmt"
	"strings"
	"time"

	"github.com/jbsmith7741/go-tools/appenderr"
)

type TimeFrame struct {
	Start       hour  `uri:"from" required:"true"`
	End         hour  `uri:"to"`
	EveryXHours int   `uri:"every-x-hours" default:"1"`
	OnHours     []int `uri:"on-hours"`
	OffHours    []int `uri:"off-hours"`
}

type hour struct {
	time.Time
}

func (h *hour) UnmarshalJSON(b []byte) error {
	return h.UnmarshalText(b)
}
func (h *hour) UnmarshalText(b []byte) error {
	s := strings.Trim(string(b), `"`)
	t, err := time.Parse("2006-01-02T15", s)
	if err != nil {
		return err
	}
	h.Time = t
	return nil
}

func (h hour) MarshalText() ([]byte, error) {
	return []byte(h.String()), nil
}

func (h hour) String() string {
	return h.Format("2006-01-02T15")
}

func (tf TimeFrame) Validate() error {
	errs := appenderr.New()
	for _, v := range tf.OnHours {
		if v < 0 || v > 23 {
			errs.Add(fmt.Errorf("on hours %d invalid", v))
		}
	}
	for _, v := range tf.OffHours {
		if v < 0 || v > 23 {
			errs.Add(fmt.Errorf("off hours %d invalid", v))
		}
	}
	if tf.Start.IsZero() || tf.End.IsZero() {
		errs.Add(fmt.Errorf("start and end date must not be zero"))
	}
	return errs.ErrOrNil()
}

func (tf TimeFrame) Generate() []time.Time {
	times := make([]time.Time, 0)
	dur := time.Hour
	if tf.EveryXHours > 0 {
		dur = time.Hour * time.Duration(tf.EveryXHours)
	}

	hours := makeOnHrs(tf.OnHours, tf.OffHours)
	check := func(t time.Time) bool {
		return t.Before(tf.End.Time) || t.Equal(tf.End.Time)
	}
	if tf.End.Before(tf.Start.Time) {
		check = func(t time.Time) bool {
			return t.After(tf.End.Time) || t.Equal(tf.End.Time)
		}
		dur *= -1
	}
	for t := tf.Start.Time; check(t); t = t.Add(dur) {
		if hours[t.Hour()] {
			times = append(times, t)
		}
	}

	return times
}

func makeOnHrs(onHrs, offHrs []int) []bool {
	finalHrs := make([]bool, 24)

	if len(onHrs) == 0 {
		for i := 0; i < 24; i++ {
			finalHrs[i] = true
		}
	} else {
		for _, v := range onHrs {
			finalHrs[v] = true
		}
	}

	// 'subtract' off hours
	for _, v := range offHrs {
		finalHrs[v] = false
	}

	return finalHrs
}
