package tmpl

import (
	"fmt"
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
	if t.IsZero() {
		return s
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

	return s
}

// PathTime will attempt to extract a time value from the path
// by the following formats
// filename - /path/{20060102T150405}.txt
// hour slug - /path/2006/01/02/15/file.txt
// day slug - /path/2006/01/02/file.txt
// month slug - /path/2006/01/file.txt
func PathTime(pth string) time.Time {
	srcDir, srcFile := filepath.Split(pth)

	// filename regex
	re := regexp.MustCompile(`[0-9]{8}T[0-9]{6}`)
	srcTS := re.FindString(srcFile)

	// hour slug regex
	hSlugRe := regexp.MustCompile(`[0-9]{4}\/[0-9]{2}\/[0-9]{2}\/[0-9]{2}`)
	hSrcTS := hSlugRe.FindString(srcDir)

	// day slug regex
	dSlugRe := regexp.MustCompile(`[0-9]{4}\/[0-9]{2}\/[0-9]{2}`)
	dSrcTS := dSlugRe.FindString(srcDir)

	// month slug regex
	mSlugRe := regexp.MustCompile(`[0-9]{4}\/[0-9]{2}`)
	mSrcTS := mSlugRe.FindString(srcDir)

	// discover the source path timestamp from the following
	// supported formats.
	var t time.Time
	if srcTS != "" {
		// src ts in filename
		tsFmt := "20060102T150405" // output format
		t, _ = time.Parse(tsFmt, srcTS)
	} else if hSrcTS != "" {
		// src ts in hour slug
		hFmt := "2006/01/02/15"
		t, _ = time.Parse(hFmt, hSrcTS)
	} else if dSrcTS != "" {
		// src ts in day slug
		dFmt := "2006/01/02"
		t, _ = time.Parse(dFmt, dSrcTS)
	} else if mSrcTS != "" {
		// src ts in month slug
		mFmt := "2006/01"
		t, _ = time.Parse(mFmt, mSrcTS)
	}

	return t
}
