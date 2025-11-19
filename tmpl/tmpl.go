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
	"github.com/pcelvng/task"
)

const DateHour = "2006-01-02T15"

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
// Template values are case-sensitive.
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

	minute := fmt.Sprintf("%02d", t.Minute())
	s = strings.ReplaceAll(s, "{min}", minute)

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
	hourRegex      = regexp.MustCompile(`(?:date|hour|hour_utc)[=:](\d{4}-\d{2}-\d{2}T\d{2})`)
	timestampRegex = regexp.MustCompile(`(?:day|date|time|timestamp)[=:](\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)`)
)

// TaskTime will get the task process time by
// 1. use the metadata `cron` timestamp
// 2. look for specific values in the info query params
// 3. look at the file path to pull the timestamp
func TaskTime(t task.Task) time.Time {
	q, _ := url.ParseQuery(t.Meta)
	if t, err := time.Parse(DateHour, q.Get("cron")); err == nil {
		return t
	}

	return InfoTime(t.Info)
}

// InfoTime will attempting to pull a timestamp from the info string
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

	if v := timestampRegex.FindStringSubmatch(info); len(v) > 1 {
		t, err := time.Parse(time.RFC3339, v[1])
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
	if v := dayRegex.FindStringSubmatch(info); len(v) > 1 {
		t, err := time.Parse("2006-01-02", v[1])
		if err == nil {
			return t
		}
		log.Println(err)
	}

	return PathTime(u.Path)
}

var regexMeta = regexp.MustCompile(`{meta:(\w+)}`)

// Meta populates a template string with the data provided in the Getter map
// it replaces values of `{meta:key}` with the value in the map[key]
// and returns a list of all matching keys in the template
func Meta(tmpl string, meta Getter) (s string, keys []string) {
	for _, match := range regexMeta.FindAllStringSubmatch(tmpl, -1) {
		// replace the original match with the meta value from the key
		v, key := match[0], match[1]
		keys = append(keys, key)
		tmpl = strings.Replace(tmpl, v, meta.Get(key), -1)
	}

	return tmpl, keys
}

type GetMap map[string]any
type TMap[T any] map[string]T

func (t TMap[any]) Get(k string) string {
	return fmt.Sprintf("%v", t[k])
}

func (m GetMap) Get(k string) string {
	switch v := m[k].(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case nil:
		return ""
	}
	return fmt.Sprintf("i~%T", m[k])
}

type Getter interface {
	Get(string) string
}

type granularity int

const (
	granularityHourly granularity = iota
	granularityDaily
	granularityMonthly
)

// isConsecutive checks if two times are consecutive based on the granularity
func isConsecutive(t1, t2 time.Time, gran granularity) bool {
	// Equal times are always consecutive (handles duplicates)
	if t1.Equal(t2) {
		return true
	}
	
	switch gran {
	case granularityHourly:
		return t2.Sub(t1) == time.Hour
	case granularityDaily:
		// Check if next calendar day (not exactly 24 hours)
		y1, m1, d1 := t1.Date()
		y2, m2, d2 := t2.Date()
		// Add one day to t1 and check if it matches t2's date
		nextDay := time.Date(y1, m1, d1+1, 0, 0, 0, 0, t1.Location())
		yn, mn, dn := nextDay.Date()
		return y2 == yn && m2 == mn && d2 == dn
	case granularityMonthly:
		// Check if next month
		y1, m1, _ := t1.Date()
		y2, m2, _ := t2.Date()
		expectedYear := y1
		expectedMonth := m1 + 1
		if expectedMonth > 12 {
			expectedMonth = 1
			expectedYear++
		}
		return y2 == expectedYear && m2 == expectedMonth
	}
	return false
}

// formatTime formats a time based on granularity
func formatTime(t time.Time, gran granularity) string {
	switch gran {
	case granularityMonthly:
		return t.Format("2006/01")
	case granularityDaily:
		return t.Format("2006/01/02")
	case granularityHourly:
		return t.Format("2006/01/02T15")
	}
	return t.Format("2006/01/02T15")
}

// PrintDates takes a slice of times and displays the range of times in a more friendly format.
// It automatically detects the granularity (hourly/daily/monthly) and formats accordingly.
// Examples:
//   - Hourly: "2006/01/02T15-2006/01/02T18"
//   - Daily: "2006/01/02-2006/01/05"
//   - Monthly: "2006/01-2006/04"
//   - Mixed: "2006/01-2006/03, 2006/05/01T10"
func PrintDates(dates []time.Time) string {
	if len(dates) == 0 {
		return ""
	}

	// Sort dates
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })

	// Single timestamp - return full hour format
	if len(dates) == 1 {
		return dates[0].Format("2006/01/02T15")
	}

	// Detect granularity in a single pass (skip duplicates)
	monthMap := make(map[string]bool)
	dayMap := make(map[string]bool)
	gran := granularityMonthly // Start optimistic, downgrade as needed
	
	for i, t := range dates {
		// Skip duplicates for granularity detection
		if i > 0 && t.Equal(dates[i-1]) {
			continue
		}
		
		// Detect granularity while iterating
		monthKey := t.Format("2006-01")
		dayKey := t.Format("2006-01-02")
		
		// If we've seen this month before, it's not monthly data
		if monthMap[monthKey] && gran == granularityMonthly {
			gran = granularityDaily
		}
		// If we've seen this day before, it's hourly data
		if dayMap[dayKey] && gran == granularityDaily {
			gran = granularityHourly
		}
		
		monthMap[monthKey] = true
		dayMap[dayKey] = true
	}

	// Build output
	var result strings.Builder
	rangeStart := dates[0]
	prev := dates[0]
	inRange := false

	for i := 1; i < len(dates); i++ {
		curr := dates[i]
		
		if isConsecutive(prev, curr, gran) {
			// Continue range
			inRange = true
			prev = curr
			continue
		}
		
		// Range broken or gap - write the previous range/item
		if inRange {
			// Close the range (but check if it's just duplicates)
			if rangeStart.Equal(prev) {
				// Just duplicates, write as single item
				result.WriteString(rangeStart.Format("2006/01/02T15"))
			} else {
				result.WriteString(formatTime(rangeStart, gran))
				result.WriteString("-")
				result.WriteString(formatTime(prev, gran))
			}
		} else {
			// Single item in a multi-timestamp dataset - use hour format
			result.WriteString(rangeStart.Format("2006/01/02T15"))
		}
		result.WriteString(",")
		
		// Start new range
		rangeStart = curr
		inRange = false
		prev = curr
	}

	// Handle the last item/range
	if inRange {
		// Close final range (but check if it's just duplicates)
		if rangeStart.Equal(prev) {
			// Just duplicates, write as single item
			result.WriteString(rangeStart.Format("2006/01/02T15"))
		} else {
			result.WriteString(formatTime(rangeStart, gran))
			result.WriteString("-")
			result.WriteString(formatTime(prev, gran))
		}
		return result.String()
	}
	
	// Single final item in multi-timestamp dataset - use hour format
	result.WriteString(rangeStart.Format("2006/01/02T15"))
	return result.String()
}
