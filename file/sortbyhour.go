package file

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
)

var (
	defaultSep     = ","
	defaultDateFmt = time.RFC3339 // "2006-01-02T15:04:05Z07:00"
)

func NewSortByHourConfig(srcPath, destTmpl, dateField string) *SortByHourConfig {
	return &SortByHourConfig{
		WriteByHourConfig: NewWriteByHourConfig(destTmpl),
		SrcPath:           srcPath,
		TimeField:         dateField,
		TimeFmt:           defaultDateFmt,
		Sep:               defaultSep,
	}
}

func NewWriteByHourConfig(destTmpl string) *WriteByHourConfig {
	return &WriteByHourConfig{
		DestTmpl: destTmpl,
	}
}

type SortByHourConfig struct {
	*WriteByHourConfig
	SrcFormat      string // line format (json, csv); if blank will attempt to guess from the file extension
	SrcCompression string // file compression; if blank will attempt to guess based on file extension
	SrcPath        string // full absolute source file path
	TimeField      string // string date field (CSV is an index offset - uint parseable)
	TimeFmt        string // date field date/time format
	Sep            string // csv field separator (if using csv)
}

type WriteByHourConfig struct {
	DestTmpl        string // sorted file final path template
	DestCompression string // destination file compression; if blank will attempt to guess based on template file extension
}

func NewSortByHour(config *SortByHourConfig) (*SortByHour, error) {
	reader, err := NewStatsReader(config.SrcPath, nil)
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
		config:     config,
		srcPath:    config.SrcPath,
		reader:     reader,
		hourWriter: hourWriter,
		parseLine:  lineParser,
	}

	return sByHour, nil
}

type SortByHour struct {
	config     *SortByHourConfig
	srcPath    string // use path object
	reader     StatsReadCloser
	hourWriter *WriteByHour
	parseLine  fieldParser
}

func NewWriteByHour(config *WriteByHourConfig) (*WriteByHour, error) {
	return &WriteByHour{
		config:       config,
		destTemplate: config.DestTmpl,
	}, nil
}

// WriteByHour handles writing to multiple files to the correct hour path
// WriteByHour implements StatsWriter but also implements methods for
// individual file stats.
type WriteByHour struct {
	config       *WriteByHourConfig
	destTemplate string
	writers      map[string]StatsWriteCloser // the file path/name is the map key
	mu           sync.Mutex
}

func (w *WriteByHour) WriteLine(l []byte, t time.Time) (err error) {
	pth := tmpl.FmtTemplate(w.destTemplate, t)
	w.mu.Lock()
	writer, found := w.writers[pth]
	if !found {
		// create writer - doesn't exist
		var err error
		writer, err = NewStatsWriter(pth, nil)
		if err != nil {
			return err
		}

		w.writers[pth] = writer
	}
	w.mu.Unlock()
	return writer.WriteLine(l)
}

// Stats will provide sums of all underlying writers
func (w *WriteByHour) Stats() stat.Stat {
	return stat.Stat{}
}

// AllStats will provide stats broken down by file.
func (w *WriteByHour) AllStats() []stat.Stat {
	var stats []stat.Stat
	for _, writer := range w.writers {
		stats = append(stats, writer.Stats())
	}
	return stats
}

func (w *WriteByHour) Close() error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(w.writers))
	for _, writer := range w.writers {
		wg.Add(1)
		go func() {
			err := writer.Close()
			if err != nil {
				errChan <- err
			}
			wg.Done()
		}()
	}

	wg.Wait()
	close(errChan)
	for err := range errChan {
		return err // just return the first error
	}

	return nil
}

// getLineFieldParser will get the parser based on the following logic:
//
// 1. Check config.SrcFormat
// 2. Check extension (tsv, csv, psv, json)
// 3. Check for a non-zero value for config.Sep
func getLineParser(config *SortByHourConfig) (fieldParser, error) {
	sep := defaultSep
	var timeFieldIndex int
	var err error

	// check SrcFormat
	switch config.SrcFormat {
	case "csv", "tsv", "psv":
		goto CSV
	case "json":
		goto JSON
	}

	// check extension
	switch getExt(config.SrcPath) {
	case ".csv", ".tsv", ".psv":
		goto CSV
	case ".json":
		goto JSON
	}

	// check separator
	if config.Sep != "" {
		goto CSV
	} else {
		goto JSON // default when all else fails
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

	timeFieldIndex, err = strconv.Atoi(config.TimeField)
	if err != nil {
		return nil, err
	}

	if timeFieldIndex < 0 {
		return nil, errors.New(fmt.Sprintf("csv time field index must not be negative, got '%v'", config.TimeField))
	}

	return newCSVParser(sep, timeFieldIndex, config.TimeFmt).parseLine, nil
JSON:
	return newJsonParser(config.TimeField, config.TimeFmt).parseLine, nil
}

// getExt will retrieve the file extension that
// is not extension related to compression.
func getExt(p string) string {
	p = strings.Replace(p, ".gz", "", 1)
	return path.Ext(p)
}

// getExtCompression will retrieve the file compression
// extension. It assumes only the last extension
// will contain the compression extension if a compression
// extension is represented at all.
//
// Note: at the moment only gz extension is supported.
// Possibly others will be supported in the future.
func getExtCompresion(p string) string {
	ext := path.Ext(p)
	switch ext {
	case ".gz":
		return ext
	}

	return ""
}

type fieldParser func([]byte) (time.Time, error)

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
		separator:  sep,
		format:     timeFmt,
	}
}

type csvParser struct {
	separator  string
	fieldIndex int // date field column index (0 indexed)
	format     string
}

func (p *csvParser) parseLine(b []byte) (time.Time, error) {
	var t time.Time
	s := strings.Split(string(b), p.separator)
	if len(s) < p.fieldIndex {
		return t, fmt.Errorf("field index '%v' not in '%v'", p.fieldIndex, string(b))
	}

	return time.Parse(p.format, s[p.fieldIndex])
}

func newJsonParser(field, timeFmt string) *jsonParser {
	return &jsonParser{
		field:  field,
		format: timeFmt,
	}
}

type jsonParser struct {
	field  string // json time field
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
