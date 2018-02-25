package tmpl

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	regYear      = regexp.MustCompile(`{(Y|y){4}}`)
	regYearShort = regexp.MustCompile(`{(Y|y){2}}`)
	regMonth     = regexp.MustCompile(`{(M|m){2}}`)
	regDay       = regexp.MustCompile(`{(D|d){2}}`)
	regHour      = regexp.MustCompile(`{(H|h){2}}`)
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
func Parse(s string, t time.Time) string {
	// {DAY_SLUG}
	s = strings.Replace(s, "{DAY_SLUG}", "{YYYY}/{MM}/{DD}", -1)

	// {SLUG}
	s = strings.Replace(s, "{SLUG}", "{YYYY}/{MM}/{DD}/{HH}", -1)

	// {TS}
	ts := t.Format("20060102T150405")
	s = strings.Replace(s, "{TS}", ts, -1)

	y, m, d := t.Date()
	year := strconv.Itoa(y)
	s = regYear.ReplaceAllString(s, year)
	s = regYearShort.ReplaceAllString(s, year[2:])

	month := fmt.Sprintf("%02d", m)
	s = regMonth.ReplaceAllString(s, month)

	day := fmt.Sprintf("%02d", d)
	s = regDay.ReplaceAllString(s, day)

	s = regHour.ReplaceAllString(s, strconv.Itoa(t.Hour()))

	return s
}
