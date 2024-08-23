package tmpl

import (
	"net/url"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func TestParse(t *testing.T) {
	hostName = "test-hostname-abcdefghij-12345"
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
			template: "{min}",
			time:     tm,
			expected: "17",
		},
		{
			template: "{YYYY}/{MM}/{DD}/{HH}",
			time:     tm,
			expected: "2018/01/02/03",
		},
		{
			template: "{YYYY}/{MM}/{DD}/{HH}/{min}",
			time:     tm,
			expected: "2018/01/02/03/17",
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
		{
			template: "path/{host}.log",
			time:     tm,
			expected: "path/" + hostName + ".log",
		},
		{
			template: "path/{POD}.json",
			time:     tm,
			expected: "path/abcdefghij-12345.json",
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
		{
			msg:     "date with -",
			path:    "/path/to/file/2018-01-17",
			expTime: trial.TimeDay("2018-01-17"),
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

func TestParseUUID(t *testing.T) {
	path := "path/to/file-{uuid}.json"
	s := Parse(path, trial.TimeDay("2020-02-10"))
	regMatch := regexp.MustCompile(`path\/to\/file-[0-9a-f]{8}[.]json`)
	if !regMatch.MatchString(s) {
		t.Errorf("FAIL: expected a random uuid: %s", s)
	}
}

func TestTaskTime(t *testing.T) {
	fn := func(in task.Task) (time.Time, error) {
		return TaskTime(in), nil
	}
	cases := trial.Cases[task.Task, time.Time]{
		"cron": {
			Input:    task.Task{Meta: "cron=2024-08-18T12"},
			Expected: trial.TimeHour("2024-08-18T12"),
		},
		"invalid-cron": {
			Input:    task.Task{Meta: "cron=2024-08-180T12"},
			Expected: time.Time{},
		},
		"param-hour": {
			Input:    task.Task{Info: "?hour=2024-01-02T13"},
			Expected: trial.TimeHour("2024-01-02T13"),
		},
		"file-path": {
			Input:    task.Task{Info: "s3://path/2024/08/02/07"},
			Expected: trial.TimeHour("2024-08-02T07"),
		},
		"priority-check": {
			Input: task.Task{
				Meta: "cron=2024-08-18T12",
				Info: "s3://path/2024/08/02/07?hour=2024-01-02T13",
			},
			Expected: trial.TimeHour("2024-08-18T12"),
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestInfoTime(t *testing.T) {
	fn := func(in string) (time.Time, error) {
		return InfoTime(in), nil
	}
	cases := trial.Cases[string, time.Time]{
		"day": {
			Input:    "?day=2020-03-05",
			Expected: trial.TimeDay("2020-03-05"),
		},
		"day map": {
			Input:    "?map=day:2020-03-05",
			Expected: trial.TimeDay("2020-03-05"),
		},
		"date": {
			Input:    "?date=2020-03-05",
			Expected: trial.TimeDay("2020-03-05"),
		},
		"date map": {
			Input:    "?map=date:2020-03-05",
			Expected: trial.TimeDay("2020-03-05"),
		},
		"date full": {
			Input:    "?date=2020-03-05T15:16:17Z",
			Expected: trial.Time(time.RFC3339, "2020-03-05T15:16:17Z"),
		},
		"date hour": {
			Input:    "?date=2020-03-05T15",
			Expected: trial.TimeHour("2020-03-05T15"),
		},

		"hour": {
			Input:    "?date=something&hour=2020-03-05T11",
			Expected: trial.TimeHour("2020-03-05T11"),
		},
		"hour extended": {
			Input:    "?date=something&hour=2020-03-05T11:12:15Z",
			Expected: trial.TimeHour("2020-03-05T11"),
		},
		"hour map": {
			Input:    "?date=something&map=hour_utc:2020-03-05T11",
			Expected: trial.TimeHour("2020-03-05T11"),
		},
		"time": {
			Input:    "?time=2020-03-05T11:15:22Z",
			Expected: trial.Time(time.RFC3339, "2020-03-05T11:15:22Z"),
		},
		"timestamp map": {
			Input:    "?map=timestamp:2020-03-05T11:15:22Z",
			Expected: trial.Time(time.RFC3339, "2020-03-05T11:15:22Z"),
		},
		"path": {
			Input:    "gs://path/2020/03/05/file.txt",
			Expected: trial.TimeDay("2020-03-05"),
		},
		"priority path": {
			Input:    "gs://path/2020/03/05/file.txt?day=2000-01-02",
			Expected: trial.TimeDay("2000-01-02"),
		},
		"priority file": {
			Input:    "gs://path/2020/03/05/file.txt?data=2000-01-02",
			Expected: trial.TimeDay("2020-03-05"),
		},
		"invalid time": {
			Input:    "?day=alksdfj",
			Expected: time.Time{},
		},
		"no time": {
			Input:    "s3://path/to/file.txt?date=something",
			Expected: time.Time{},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestParseMeta(t *testing.T) {
	type input struct {
		url      string
		m        map[string]string
		template string
	}
	type expect struct {
		Out  string
		Keys []string
	}
	fn := func(in input) (exp expect, err error) {
		var meta Getter
		if in.url != "" {
			meta, err = url.ParseQuery(in.url)
			if err != nil {
				return expect{}, err
			}
		} else {
			meta = TMap[string](in.m)
		}
		s, keys := Meta(in.template, meta)
		return expect{Out: s, Keys: keys}, nil
	}
	cases := trial.Cases[input, expect]{
		"{file}": {
			Input:    input{template: "{meta:file}", url: "file=s3://path/to/file.txt"},
			Expected: expect{Out: "s3://path/to/file.txt", Keys: []string{"file"}},
		},
		"missing key": { // populate with a blank if missing the key
			Input:    input{template: "{meta:file}"},
			Expected: expect{Keys: []string{"file"}},
		},
		"no change": {
			Input:    input{template: "the quick brown fox jumped over the lazy dog"},
			Expected: expect{Out: "the quick brown fox jumped over the lazy dog"},
		},
		"invalid match": {
			Input:    input{template: "{meta:da ta}"},
			Expected: expect{Out: "{meta:da ta}"},
		},
		"complex": {
			Input:    input{template: "{meta:file}?hour={meta:time}&key=value&pass={meta:pass}", url: "file=gs://bucket/test.gz&time=2019-03-04&pass=r$kE43"},
			Expected: expect{Out: "gs://bucket/test.gz?hour=2019-03-04&key=value&pass=r$kE43", Keys: []string{"file", "time", "pass"}},
		},
		"map": {
			Input: input{
				template: "{meta:name}&{meta:v}",
				m:        map[string]string{"name": "john", "v": "123"},
			},
			Expected: expect{Out: "john&123", Keys: []string{"name", "v"}},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestHostSlug(t *testing.T) {
	h, _ := os.Hostname()
	hostName = h

	tmp := "{HOST}"
	if r1 := Parse(tmp, time.Now()); r1 != h {
		t.Error("FAIL: invalid hostname")
	}

	tmp = "{host}"
	if r2 := Parse(tmp, time.Now()); r2 != h {
		t.Error("FAIL: invalid hostname")
	}
}

func TestPrintDates(t *testing.T) {
	f := "2006/01/02T15"
	fn := func(in []time.Time) (string, error) {
		return PrintDates(in), nil
	}
	cases := trial.Cases[[]time.Time, string]{
		"simple series": {
			Input:    trial.Times(f, "2018/04/09T03", "2018/04/09T04", "2018/04/09T05"),
			Expected: "2018/04/09T03-2018/04/09T05",
		},
		"group of dates": {
			Input:    trial.Times(f, "2018/04/10T14", "2018/04/10T14", "2018/04/10T00", "2018/04/09T00", "2018/04/10T00", "2018/04/11T00"),
			Expected: "2018/04/09T00,2018/04/10T00,2018/04/10T14,2018/04/11T00",
		},
		"missing dates in middle": {
			Input:    trial.Times(f, "2018/04/09T03", "2018/04/09T04", "2018/04/09T05", "2018/04/09T07", "2018/04/09T08", "2018/04/09T09", "2018/04/09T11"),
			Expected: "2018/04/09T03-2018/04/09T05,2018/04/09T07-2018/04/09T09,2018/04/09T11",
		},
		"daily records": {
			Input:    trial.Times(f, "2018/04/09T00", "2018/04/10T00", "2018/04/11T00", "2018/04/12T00"),
			Expected: "2018/04/09-2018/04/12",
		},
		"daily records with gaps": {
			Input:    trial.Times(f, "2018/04/09T00", "2018/04/10T00", "2018/04/11T00", "2018/04/12T00", "2018/04/15T00", "2018/04/16T00", "2018/04/17T00"),
			Expected: "2018/04/09-2018/04/12,2018/04/15-2018/04/17",
		},
	}
	trial.New(fn, cases).Test(t)

}
