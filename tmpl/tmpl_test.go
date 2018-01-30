package tmpl

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestParse(t *testing.T) {
	tm, _ := time.Parse(time.RFC3339, "2018-01-15T12:17:34Z")
	cases := []struct {
		template string
		time     time.Time
		expected string
	}{
		{
			template: "{YYYY}",
			time:     tm,
			expected: "2018",
		},
		{
			template: "{YY}",
			time:     tm,
			expected: "18",
		},
		{
			template: "{MM}",
			time:     tm,
			expected: "01",
		},
		{
			template: "{DD}",
			time:     tm,
			expected: "15",
		},
		{
			template: "{HH}",
			time:     tm,
			expected: "12",
		},
		{
			template: "{YYYY}/{MM}/{DD}/{HH}",
			time:     tm,
			expected: "2018/01/15/12",
		},
		{
			template: "{yyyy}/{mm}/{dd}/{hh}",
			time:     tm,
			expected: "2018/01/15/12",
		},
		{
			template: "{SLUG}",
			time:     tm,
			expected: "2018/01/15/12",
		},
		{
			template: "{DAY_SLUG}",
			time:     tm,
			expected: "2018/01/15",
		},
	}
	for _, test := range cases {
		result := Parse(test.template, test.time)
		if !cmp.Equal(result, test.expected) {
			t.Errorf("FAIL: %s %s", test.template, cmp.Diff(result, test.expected))
		} else {
			t.Logf("PASS: %s", test.template)
		}
	}

}
