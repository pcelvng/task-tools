package main

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

type duration int64

var regDay = regexp.MustCompile(`^([0-9]*)d$`)

func (d *duration) UnmarshalText(b []byte) error {
	dur, err := time.ParseDuration(string(b))
	if err == nil {
		*d = duration(dur)
		return nil
	}

	if regDay.MatchString(string(b)) {
		matches := regDay.FindStringSubmatch(string(b))
		i, _ := strconv.ParseInt(matches[1], 10, 64)
		*d = duration(int64(time.Hour) * 24 * i)
		return nil
	}
	return fmt.Errorf("%v is not a valid duration", string(b))
}

func (d duration) Duration() time.Duration {
	return time.Duration(d)
}
