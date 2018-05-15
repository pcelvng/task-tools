package timeframe

import (
	"fmt"
	"time"

	"github.com/jbsmith7741/go-tools/appenderr"
)

type TimeFrame struct {
	Start       time.Time `uri:"from" required:"true"`
	End         time.Time `uri:"to"`
	EveryXHours int       `uri:"every-x-hours" default:"1"`
	OnHours     []int     `uri:"on-hours"`
	OffHours    []int     `uri:"off-hours"`
}

func (tf TimeFrame) Validate() error {
	errs := appenderr.New()
	for _, v := range tf.OnHours {
		if v < 0 || v > 23 {
			errs.Add(fmt.Errorf("on hours %tf invalid", v))
		}
	}
	for _, v := range tf.OffHours {
		if v < 0 || v > 23 {
			errs.Add(fmt.Errorf("off hours %tf invalid", v))
		}
	}
	if tf.Start.IsZero() || tf.End.IsZero() {
		errs.Add(fmt.Errorf("start and end date must not be zero"))
	}
	return errs.ErrOrNil()
}

func (tf *TimeFrame) Generate() []time.Time {
	times := make([]time.Time, 0)
	dur := time.Hour
	if tf.EveryXHours != 0 {
		dur = time.Hour * time.Duration(tf.EveryXHours)
	}

	hours := makeOnHrs(tf.OnHours, tf.OffHours)
	for t := tf.Start; t.Before(tf.End) || t.Equal(tf.End); t = t.Add(dur) {
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
