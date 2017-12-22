package file

import (
	"time"
	"strings"
	"fmt"

	"github.com/buger/jsonparser"
	"github.com/pcelvng/task-tools/file/stat"
	"strconv"
	"errors"
)

var (
	defaultSep = ","
	defaultDateFmt = time.RFC3339 // "2006-01-02T15:04:05Z07:00"
)

func NewSortByHourConfig(srcPath, destTmpl, dateField string) *SortByHourConfig {
	return &SortByHourConfig{
		WriteByHourConfig: NewWriteByHourConfig(destTmpl),
		SrcPath: srcPath,
		TimeField: dateField,
		TimeFmt: defaultDateFmt,
		Sep: defaultSep,
	}
}

func NewWriteByHourConfig(destTmpl string) *WriteByHourConfig {
	return &WriteByHourConfig{
		DestTmpl: destTmpl,
	}
}

type SortByHourConfig struct {
	*WriteByHourConfig
	SrcFormat string // line format (json, csv); if blank will attempt to guess from the file extension
	SrcCompression string // file compression; if blank will attempt to guess based on file extension
	SrcPath string // full absolute source file path
	TimeField string // string date field (CSV is an index offset - uint parseable)
	TimeFmt string // date field date/time format
	Sep string // csv field separator (if using csv)
}

type WriteByHourConfig struct {
	DestTmpl string // sorted file final path template
	DestCompression string // destination file compression; if blank will attempt to guess based on template file extension
}

func NewSortByHour(config *SortByHourConfig) (*SortByHour, error) {
	reader, err := NewStatsReader()
	if err != nil {
		return nil, err
	}

	hourWriter, err := NewWriteByHour(config.WriteByHourConfig)
	if err != nil {
		return nil, err
	}

	// get correct line parser
	lineParser, err := getLineParser(config)
	if err != nil {
		return nil, err
	}

	sByHour := &SortByHour{
		config: config,
		srcPath: config.SrcPath,
		reader: reader,
		hourWriter: hourWriter,
		parseLine: lineParser,
	}

	return sByHour, nil
}

type SortByHour struct {
	config *SortByHourConfig
	srcPath string // use path object
	reader StatsReader
	hourWriter *WriteByHour
	parseLine fieldParser
}

func NewWriteByHour(config *WriteByHourConfig) (*WriteByHour, error) {
	return &WriteByHour{
		config: config,
		destTemplate: config.DestTmpl,
	}, nil
}

// WriteByHour handles writing to multiple files to the correct hour path
// WriteByHour implements StatsWriter but also implements a methods for
// individual file stats.
type WriteByHour struct {
	config *WriteByHourConfig
	destTemplate string
	writers []StatsWriter
}

func (w *WriteByHour) WriteLine(l []byte) (int64, error) {return 0, nil}

func (w *WriteByHour) Finish() error {return nil}

func (w *WriteByHour) Stats() []*stat.Stat {return nil}

func (w *WriteByHour) Close() error {return nil}

// getLineFieldParser will get the parser based on the following logic:
//
// 1. Check config.SrcFormat
// 2. Check extension (tsv, csv, psv, json)
// 3. Check for a non-zero value for config.Sep
func getLineParser(config *SortByHourConfig) (fieldParser, error) {
	sep := defaultSep
	var timeIndex int
	var err error

	switch config.SrcFormat {
	case "csv", "tsv", "psv":
		goto CSV
	case "json":
		goto JSON
	}

CSV:
	// determine sep
	// 1. Rely on config (if provided)
	// 2. Infer from src file format
	if config.Sep != "" {
		sep = config.Sep
	} else if config.SrcFormat != "" {
		switch config.SrcFormat {
		case "csv":
			sep = ","
		case "tsv":
			sep = "\t"
		case "psv":
			sep = "|"
		}
	}

	timeIndex, err = strconv.Atoi(config.TimeField)
	if err != nil {
		return nil, err
	}

	if timeIndex < 0 {
		return nil, errors.New(fmt.Sprintf("csv time field index must not be negative, got '%v'", config.TimeField))
	}

	return newCSVParser(sep, timeIndex, config.TimeFmt).parseLine, nil
JSON:
	return newJsonParser(config.TimeField, config.TimeFmt).parseLine, nil
}

func newCSVParser(sep string, fieldIndex int, timeFmt string) *csvParser {
	if fieldIndex < 0 {
		fieldIndex = 0
	}

	if sep == "" {
		sep = defaultSep
	}

	if timeFmt == "" {
		timeFmt = defaultDateFmt
	}


	return &csvParser{
		fieldIndex: fieldIndex,
		separator: sep,
		format: timeFmt,
	}
}

func newJsonParser(field, timeFmt string) *jsonParser {
	return &jsonParser{
		field: field,
		format: timeFmt,
	}
}

type fieldParser func([]byte) (time.Time, error)

type csvParser struct {
	separator string
	fieldIndex int // date field column index (0 indexed)
	format string
}

func (p *csvParser) parseLine(b []byte) (time.Time, error) {
	var t time.Time
	s := strings.Split(string(b), p.separator)
	if len(s) < p.fieldIndex {
		return t, fmt.Errorf("field index '%v' not in '%v'", p.fieldIndex, string(b))
	}

	return time.Parse(p.format, s[p.fieldIndex])
}

type jsonParser struct {
	field string // json time field
	format string
}

func (p *jsonParser) parseLine(b []byte) (time.Time, error) {
	var t time.Time
	s, err := jsonparser.GetString(b, p.field)
	if err != nil {
		return t, fmt.Errorf("field '%v' not in '%v'", p.field, string(b))
	}

	return time.Parse(p.format, s)
}



