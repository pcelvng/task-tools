package tmpl

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	regYear             = regexp.MustCompile(`{(Y|y){4}}`)
	regYearShort        = regexp.MustCompile(`{(Y|y){2}}`)
	regMonth            = regexp.MustCompile(`{(M|m){2}}`)
	regDay              = regexp.MustCompile(`{(D|d){2}}`)
	regHour             = regexp.MustCompile(`{(H|h){2}}`)
	regHost             = regexp.MustCompile(`(?i){host}`)
	regPod              = regexp.MustCompile(`(?i){pod}`)
	hostName     string = "hostname"
)

func init() {
	h, err := os.Hostname()
	if err == nil {
		hostName = h
	}
}

// Parse will parse a template string according to the provided
// instance of time.Time. It supports the following
// template tokens:
//
// {YYYY} (year - four digits: ie 2017)
// {YY}   (year - two digits: ie 17)
// {MM}   (month - two digits: ie 12)
// {DD}   (day - two digits: ie 13)
// {HH}   (hour - two digits: ie 00)
// {min}   (minute - two digits: ie 00)
// {TS}   (timestamp in the format 20060102T150405)
// {SLUG} (alias of HOUR_SLUG)
// {HOUR_SLUG} (date hour slug, shorthand for {YYYY}/{MM}/{DD}/{HH})
// {DAY_SLUG} (date day slug, shorthand for {YYYY}/{MM}/{DD})
// {MONTH_SLUG} (date month slug, shorthand for {YYYY}/{MM})
// {HOST} (os hostname)
// {POD}  kubernetes unique pod name
// {UUID} creates an 8 character unique id
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

	min := fmt.Sprintf("%02d", t.Minute())
	s = strings.ReplaceAll(s, "{min}", min)

	// {HOST}
	s = regHost.ReplaceAllString(s, hostName)

	// {POD} - only keep the unique node-pod ids
	v := strings.Split(hostName, "-")
	if len(v) > 1 {
		v = v[len(v)-2:]
	}
	h := strings.Join(v, "-")
	s = regPod.ReplaceAllString(s, h)

	// {UUID}
	if strings.Contains(strings.ToLower(s), "{uuid}") {
		id := strings.Split(uuid.New().String(), "-")[0]
		s = strings.Replace(s, "{uuid}", id, 1)
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
		s := d2SlugRe.FindString(pth)
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

var (
	dayRegex       = regexp.MustCompile(`(?:day|date)[=:](\d{4}-\d{2}-\d{2})`)
	hourRegex      = regexp.MustCompile(`(?:hour|hour_utc)[=:](\d{4}-\d{2}-\d{2}T\d{2})`)
	timestampRegex = regexp.MustCompile(`time(?:stamp)?[=:](\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)`)
)

// InfoTime will attempting to pull a timestamp from a info string
// through the query params and file path and name
// Order of priority and supported formats
// ?day=2006-01-02
// ?hour=2006-01-02T15
// ?time=2005-01-02T15:04:05Z
// file name 20060102T150405.txt
// path layout 2006/01/02/15 or "2006/01/02 or "2006/01
func InfoTime(info string) time.Time {
	u, err := url.Parse(info)
	if err != nil {
		return time.Time{}
	}

	if v := dayRegex.FindStringSubmatch(info); len(v) > 1 {
		t, err := time.Parse("2006-01-02", v[1])
		if err == nil {
			return t
		}
	}
	if v := hourRegex.FindStringSubmatch(info); len(v) > 1 {
		t, err := time.Parse("2006-01-02T15", v[1])
		if err == nil {
			return t
		}
	}
	if v := timestampRegex.FindStringSubmatch(info); len(v) > 1 {
		t, err := time.Parse(time.RFC3339, v[1])
		if err == nil {
			return t
		}
		log.Println(err)
	}

	return PathTime(u.Path)
}

var regexMeta = regexp.MustCompile(`{meta:(\w+)}`)

// Meta will parse a template string according to the provided
// metadata query params
// all token should be prefixed with meta
// {meta:key}
func Meta(s string, meta url.Values) string {
	for _, match := range regexMeta.FindAllStringSubmatch(s, -1) {
		// replace the original match with the meta value from the key
		v, key := match[0], match[1]
		s = strings.Replace(s, v, meta.Get(key), -1)
	}

	return s
}

// PrintDates takes a slice of times and displays the range of times in a more friendly format.
func PrintDates(dates []time.Time) string {
	tFormat := "2006/01/02T15"
	if len(dates) == 0 {
		return ""
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })
	prev := dates[0]
	s := prev.Format(tFormat)
	series := false
	for _, t := range dates {
		diff := t.Truncate(time.Hour).Sub(prev.Truncate(time.Hour))
		if diff != time.Hour && diff != 0 {
			if series {
				s += "-" + prev.Format(tFormat)
			}
			s += "," + t.Format(tFormat)
			series = false
		} else if diff == time.Hour {
			series = true
		}
		prev = t
	}
	if series {
		s += "-" + prev.Format(tFormat)
	}

	//check for daily records only
	if !strings.Contains(s, "-") {
		days := strings.Split(s, ",")
		prev, _ := time.Parse(tFormat, days[0])
		dailyString := prev.Format("2006/01/02")
		series = false

		for i := 1; i < len(days); i++ {
			tm, _ := time.Parse(tFormat, days[i])
			if r := tm.Sub(prev) % (24 * time.Hour); r != 0 {
				return s
			}
			if tm.Sub(prev) != 24*time.Hour {
				if series {
					dailyString += "-" + prev.Format("2006/01/02")
					series = false
				}
				dailyString += "," + tm.Format("2006/01/02")

			} else {
				series = true
			}
			prev = tm
		}
		if series {
			return dailyString + "-" + prev.Format("2006/01/02")
		}
		return dailyString
	}
	return s
}
