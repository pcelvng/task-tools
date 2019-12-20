package tmpl

import (
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hydronica/trial"
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
			expTime: trial.TimeHour("2018-02-14T14"),
		},
		{
			msg:     "hour slug matching",
			path:    "/path/to/file/2018/02/14/14/file.txt",
			expTime: trial.TimeHour("2018-02-14T14"),
		},
		{
			msg:     "hour slug file match",
			path:    "/path/to/file/2018/02/14/14.txt",
			expTime: trial.TimeHour("2018-02-14T14"),
		},
		{
			msg:     "day slug matching",
			path:    "/path/to/file/2018/01/07/file.txt",
			expTime: trial.TimeHour("2018-01-07T00"),
		},
		{
			msg:     "day file matching",
			path:    "/path/to/file/2018/01/07.txt",
			expTime: trial.TimeHour("2018-01-07T00"),
		},
		{
			msg:     "month slug matching",
			path:    "/path/to/file/2017/12/file.txt",
			expTime: trial.TimeHour("2017-12-01T00"),
		},
		{
			msg:     "month file matching",
			path:    "/path/to/file/2017/12.txt",
			expTime: trial.TimeHour("2017-12-01T00"),
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

func TestParseMeta(t *testing.T) {
	fn := func(v trial.Input) (interface{}, error) {
		template := v.Slice(0).String()
		meta, err := url.ParseQuery(v.Slice(1).String())
		if err != nil {
			return nil, err
		}
		return Meta(template, meta), nil
	}
	cases := trial.Cases{
		"{file}": {
			Input:    trial.Args("{meta:file}", "file=s3://path/to/file.txt"),
			Expected: "s3://path/to/file.txt",
		},
		"missing key": { // populate with a blank if missing the key
			Input:    trial.Args("{meta:file}", ""),
			Expected: "",
		},
		"no change": {
			Input:    trial.Args("the quick brown fox jumped over the lazy dog", ""),
			Expected: "the quick brown fox jumped over the lazy dog",
		},
		"invalid match": {
			Input:    trial.Args("{meta:da ta}", ""),
			Expected: "{meta:da ta}",
		},
		"complex": {
			Input:    trial.Args("{meta:file}?hour={meta:time}&key=value&pass={meta:pass}", "file=gs://bucket/test.gz&time=2019-03-04&pass=r$kE43"),
			Expected: "gs://bucket/test.gz?hour=2019-03-04&key=value&pass=r$kE43",
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestHostSlug(t *testing.T) {
	h, _ := os.Hostname()

	tmp := "{HOST}"
	if r1 := Parse(tmp, time.Now()); r1 != h {
		t.Error("FAIL: invalid hostname")
	}

	tmp = "{host}"
	if r2 := Parse(tmp, time.Now()); r2 != h {
		t.Error("FAIL: invalid hostname")
	}
}
