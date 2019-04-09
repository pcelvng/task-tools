package tmpl

import (
	"fmt"
	"os"
	"path/filepath"
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
	regHost      = regexp.MustCompile(`(?i){host}`)
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
// {SLUG} (alias of HOUR_SLUG)
// {HOUR_SLUG} (date hour slug, shorthand for {YYYY}/{MM}/{DD}/{HH})
// {DAY_SLUG} (date day slug, shorthand for {YYYY}/{MM}/{DD})
// {MONTH_SLUG} (date month slug, shorthand for {YYYY}/{MM})
// {HOST} (os hostname)
//
// Template values are case sensitive.
//
// Items can be commented out by a hash sign #
// anything after the # will be ignored
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
	if t.IsZero() {
		return s
	}
	var end string
	// ignore values after hash
	if i := strings.Index(s, "#"); i > 0 {
		end = s[i:]
		s = s[:i]
	}

	// {SLUG}
	s = strings.Replace(s, "{SLUG}", "{HOUR_SLUG}", -1)

	// {HOUR_SLUG}
	s = strings.Replace(s, "{HOUR_SLUG}", "{YYYY}/{MM}/{DD}/{HH}", -1)

	// {DAY_SLUG}
	s = strings.Replace(s, "{DAY_SLUG}", "{YYYY}/{MM}/{DD}", -1)

	// {MONTH_SLUG}
	s = strings.Replace(s, "{MONTH_SLUG}", "{YYYY}/{MM}", -1)

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

	hour := fmt.Sprintf("%02d", t.Hour())
	s = regHour.ReplaceAllString(s, hour)

	// {HOST}
	if h, err := os.Hostname(); err == nil {
		s = regHost.ReplaceAllString(s, h)
	}

	return s + end
}

var (
	hFileRe  = regexp.MustCompile(`[0-9]{8}T[0-9]{6}`)                      // 20060102T150405
	hSlugRe  = regexp.MustCompile(`[0-9]{4}\/[0-9]{2}\/[0-9]{2}\/[0-9]{2}`) // 2006/01/02/15
	dSlugRe  = regexp.MustCompile(`[0-9]{4}\/[0-9]{2}\/[0-9]{2}`)           // 2006/01/02
	mSlugRe  = regexp.MustCompile(`[0-9]{4}\/[0-9]{2}`)                     // 2006/01
	d2SlugRe = regexp.MustCompile(`[0-9]{4}-[0-9]{2}-[0-9]{2}`)             // 2006-01-02
)

// PathTime will attempt to extract a time value from the path
// by the following formats
// filename - /path/{20060102T150405}.txt
// hour slug - /path/2006/01/02/15/file.txt
// day slash slug - /path/2006/01/02/file.txt
// day dash slug - /path/2006-01-02.txt
// month slug - /path/2006/01/file.txt
func PathTime(pth string) time.Time {
	_, srcFile := filepath.Split(pth)

	// filename regex
	if hFileRe.MatchString(srcFile) {
		s := hFileRe.FindString(srcFile)
		t, _ := time.Parse("20060102T150405", s)
		return t
	}

	// hour slug regex
	if hSlugRe.MatchString(pth) {
		s := hSlugRe.FindString(pth)
		t, _ := time.Parse("2006/01/02/15", s)
		return t
	}

	// day slash slug regex
	if dSlugRe.MatchString(pth) {
		s := dSlugRe.FindString(pth)
		t, _ := time.Parse("2006/01/02", s)
		return t
	}

	// day dash slug regex
	if d2SlugRe.MatchString(pth) {
		s := dSlugRe.FindString(pth)
		t, _ := time.Parse("2006-01-02", s)
		return t
	}

	// month slug regex
	if mSlugRe.MatchString(pth) {
		s := mSlugRe.FindString(pth)
		t, _ := time.Parse("2006/01", s)
		return t
	}

	return time.Time{}
}
