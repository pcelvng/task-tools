package tmpl

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Parse will parse a template string according to the provided
// instance of time.Time. It supports the following
// template tokens:
//
// {YYYY} (year - four digits: ie 2017)
// {YY}   (year - two digits: ie 17)
// {MM}   (month - two digits: ie 12)
// {DD}   (day - two digits: ie 13)
// {HH}   (hour - two digits: ie 00)
// {TS}   (timestamp in the format 20060102T150405)
// {SLUG} (date hour slug, shorthand for {YYYY}/{MM}/{DD}/{HH})
// {DAY_SLUG} (date day slug, shorthand for {YYYY}/{MM}/{DD})
//
// Template values are case sensitive.
//
// Examples:
// template: "{YYYY}-{MM}-{DD}T{HH}:00"
// could return: "2017-01-01T23:00"
//
// template: "{TS}"
// could return: "20170101T230101"
//
// template: "base/path/{SLUG}/records-{TS}.json.gz"
// could return: "base/path/2017/01/01/23/records-20170101T230101.json.gz"
func Parse(tmplt string, t time.Time) string {
	// {DAY_SLUG}
	tmplt = strings.Replace(tmplt, "{DAY_SLUG}", "{YYYY}/{MM}/{DD}", -1)

	// {SLUG}
	tmplt = strings.Replace(tmplt, "{SLUG}", "{YYYY}/{MM}/{DD}/{HH}", -1)

	// {TS}
	ts := t.Format("20060102T150405")
	s := strings.Replace(tmplt, "{TS}", ts, -1)

	// {YYYY}
	y := strconv.Itoa(t.Year())
	s = strings.Replace(tmplt, "{YYYY}", y, -1)

	// {YY}
	s = strings.Replace(s, "{YY}", y[2:], -1)

	// {MM}
	m := fmt.Sprintf("%02d", int(t.Month()))
	s = strings.Replace(s, "{MM}", m, -1)

	// {DD}
	d := fmt.Sprintf("%02d", t.Day())
	s = strings.Replace(s, "{DD}", d, -1)

	// {HH}
	h := fmt.Sprintf("%02d", t.Hour())
	s = strings.Replace(s, "{HH}", h, -1)

	return s
}
