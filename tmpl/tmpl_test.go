package tmpl

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jbsmith7741/trial"
)

func TestParse(t *testing.T) {
	tm, _ := time.Parse(time.RFC3339, "2018-01-02T03:17:34Z")
	tmZero := time.Time{}
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
			expected: "02",
		},
		{
			template: "{HH}",
			time:     tm,
			expected: "03",
		},
		{
			template: "{YYYY}/{MM}/{DD}/{HH}",
			time:     tm,
			expected: "2018/01/02/03",
		},
		{
			template: "{yyyy}/{mm}/{dd}/{hh}",
			time:     tm,
			expected: "2018/01/02/03",
		},
		{
			template: "{SLUG}",
			time:     tm,
			expected: "2018/01/02/03",
		},
		{
			template: "{DAY_SLUG}",
			time:     tm,
			expected: "2018/01/02",
		},
		{
			template: "",
			time:     tm,
			expected: "",
		},
		{
			template: "./file.txt",
			time:     tm,
			expected: "./file.txt",
		},
		{
			template: "./file.txt",
			time:     tmZero,
			expected: "./file.txt",
		},
		{
			template: "path/{yyyy}/{mm}/{dd}#{yyyy}/{mm}",
			time:     trial.Time("2006-01-02", "2018-05-10"),
			expected: "path/2018/05/10#{yyyy}/{mm}",
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

func TestPathTime(t *testing.T) {
	cases := []struct {
		msg     string
		path    string
		expTime time.Time
	}{
		{
			msg:     "filename parsing",
			path:    "/path/to/file/20180214T140000.txt",
			expTime: mustParse("2018-02-14T14:00:00Z"),
		},
		{
			msg:     "hour slug matching",
			path:    "/path/to/file/2018/02/14/14/file.txt",
			expTime: mustParse("2018-02-14T14:00:00Z"),
		},
		{
			msg:     "hour slug file match",
			path:    "/path/to/file/2018/02/14/14.txt",
			expTime: mustParse("2018-02-14T14:00:00Z"),
		},
		{
			msg:     "day slug matching",
			path:    "/path/to/file/2018/01/07/file.txt",
			expTime: mustParse("2018-01-07T00:00:00Z"),
		},
		{
			msg:     "day file matching",
			path:    "/path/to/file/2018/01/07.txt",
			expTime: mustParse("2018-01-07T00:00:00Z"),
		},
		{
			msg:     "month slug matching",
			path:    "/path/to/file/2017/12/file.txt",
			expTime: mustParse("2017-12-01T00:00:00Z"),
		},
		{
			msg:     "month file matching",
			path:    "/path/to/file/2017/12.txt",
			expTime: mustParse("2017-12-01T00:00:00Z"),
		},
	}
	for _, test := range cases {
		tm := PathTime(test.path)
		if !cmp.Equal(tm, test.expTime) {
			t.Errorf("FAIL: %q %s", test.msg, cmp.Diff(tm, test.expTime))
		} else {
			t.Logf("PASS: %q", test.msg)
		}
	}
}

func mustParse(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
