package timeframe

import (
	"errors"
	"testing"

	"github.com/jbsmith7741/go-tools/trial"
)

func TestTimeFrame_Generate(t *testing.T) {
	fn := func(args ...interface{}) (interface{}, error) {
		return args[0].(*TimeFrame).Generate(), nil
	}
	trial.New(fn, trial.Cases{
		"normal 24 hour generation": {
			Input: &TimeFrame{
				Start: trial.TimeDay("2018-05-01"),
				End:   trial.TimeDay("2018-05-02"),
			},
			Expected: trial.Times("2006-01-02T15",
				"2018-05-01T00",
				"2018-05-01T01",
				"2018-05-01T02",
				"2018-05-01T03",
				"2018-05-01T04",
				"2018-05-01T05",
				"2018-05-01T06",
				"2018-05-01T07",
				"2018-05-01T08",
				"2018-05-01T09",
				"2018-05-01T10",
				"2018-05-01T11",
				"2018-05-01T12",
				"2018-05-01T13",
				"2018-05-01T14",
				"2018-05-01T15",
				"2018-05-01T16",
				"2018-05-01T17",
				"2018-05-01T18",
				"2018-05-01T19",
				"2018-05-01T20",
				"2018-05-01T21",
				"2018-05-01T22",
				"2018-05-01T23",
				"2018-05-02T00"),
		},
		"every 7 hours": {
			Input: &TimeFrame{
				Start:       trial.TimeDay("2018-05-01"),
				End:         trial.TimeDay("2018-05-02"),
				EveryXHours: 7,
			},
			Expected: trial.Times("2006-01-02T15",
				"2018-05-01T00",
				"2018-05-01T07",
				"2018-05-01T14",
				"2018-05-01T21",
			),
		},
		"every 2 hours, not hours 4,8,12": {
			Input: &TimeFrame{
				Start:       trial.TimeHour("2018-05-01T00"),
				End:         trial.TimeHour("2018-05-01T12"),
				EveryXHours: 2,
				OffHours:    []int{4, 8, 12},
			},
			Expected: trial.Times("2006-01-02T15",
				"2018-05-01T00",
				"2018-05-01T02",
				"2018-05-01T06",
				"2018-05-01T10",
			),
		},
		"only prime hours": {
			Input: &TimeFrame{
				Start:   trial.TimeHour("2018-05-01T00"),
				End:     trial.TimeHour("2018-05-01T23"),
				OnHours: []int{1, 2, 3, 5, 7, 11, 13, 17, 23},
			},
			Expected: trial.Times("2006-01-02T15",
				"2018-05-01T01",
				"2018-05-01T02",
				"2018-05-01T03",
				"2018-05-01T05",
				"2018-05-01T07",
				"2018-05-01T11",
				"2018-05-01T13",
				"2018-05-01T17",
				"2018-05-01T23",
			),
		},
		"every 3 hour include evens except 6": {
			Input: &TimeFrame{
				Start:       trial.TimeHour("2018-05-01T00"),
				End:         trial.TimeHour("2018-05-01T12"),
				EveryXHours: 3,
				OnHours:     []int{0, 2, 4, 6, 8, 10, 12},
				OffHours:    []int{6},
			},
			Expected: trial.Times("2006-01-02T15",
				"2018-05-01T00",
				"2018-05-01T12",
			),
		},
		"reverse time": {
			Input: &TimeFrame{
				Start: trial.TimeHour("2018-05-01T12"),
				End:   trial.TimeHour("2018-05-01T06"),
			},
			Expected: trial.Times("2006-01-02T15",
				"2018-05-01T12",
				"2018-05-01T11",
				"2018-05-01T10",
				"2018-05-01T09",
				"2018-05-01T08",
				"2018-05-01T07",
				"2018-05-01T06",
			),
		},
	}).Test(t)
}

func TestTimeFrame_Validate(t *testing.T) {
	fn := func(args ...interface{}) (interface{}, error) {
		return nil, args[0].(*TimeFrame).Validate()
	}
	trial.New(fn, trial.Cases{
		"valid struct": {
			Input: &TimeFrame{
				Start: trial.TimeDay("2018-05-01"),
				End:   trial.TimeDay("2018-05-02"),
			},
		},
		"missing start time": {
			Input: &TimeFrame{
				End: trial.TimeDay("2018-05-02"),
			},
			ShouldErr: true,
		},
		"onHours out of range": {
			Input: &TimeFrame{
				Start:   trial.TimeDay("2018-05-01"),
				End:     trial.TimeDay("2018-05-02"),
				OnHours: []int{-1, 25},
			},
			ExpectedErr: errors.New("on hours -1 invalid"),
		},
		"offHours out of range": {
			Input: &TimeFrame{
				Start:    trial.TimeDay("2018-05-01"),
				End:      trial.TimeDay("2018-05-02"),
				OffHours: []int{1, 2, 3, 6, 25},
			},
			ExpectedErr: errors.New("off hours 25 invalid"),
		},
	}).Test(t)
}
