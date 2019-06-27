package main

import (
	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
)

var (
	taskType    = "deduper"
	description = `Deduper uniques lines of a JSON file based on a set of unique fields. Can read all files from a directory (not recursive)
and dedup to a single output file.

## Info Definition
worker info string uses a url type format:

"{src-path}?{querystring-params}"

Where:
  * src-path - is a directory or file path of file(s) to dedup.
  
  * querystring-params can be:
    - dest-template - deduped file destination and supports the following template tags:
        {SRC_FILE} (only available if deduping from a single file)
        {YYYY} (year - four digits: ie 2017)
        {YY}   (year - two digits: ie 17)
        {MM}   (month - two digits: ie 12)
        {DD}   (day - two digits: ie 13)
        {HH}   (hour - two digits: ie 00)
        {TS}   (timestamp in the format 20060102T150405)
        {SLUG} (alias of HOUR_SLUG)
        {HOUR_SLUG} (date hour slug, shorthand for {YYYY}/{MM}/{DD}/{HH})
        {DAY_SLUG} (date day slug, shorthand for {YYYY}/{MM}/{DD})
        {MONTH_SLUG} (date month slug, shorthand for {YYYY}/{MM})
        
    - fields - json or csv comma-separated field keys
    - sep - indicate CSV type file separator. If Sep is not provided then records are assumed to be json.
	- use-file-buffer - set to 'true' if file processing should use a file buffer instead of memory

Example:

 // json example
 s3://bucket/path/to/file.json?fields=f1,f2&dest-template=/usr/bin/output.json

 // csv example
 s3://bucket/path/to/file.json?fields=f1,f2&dest-template=/usr/bin/output.json&sep=,
 
 Query string params:
 - fields (field combination that makes a record unique)
 - dest-template
 - sep (optional - csv separator for csv files)

NOTE: \t or tab must be url encoded %09 
`

	appOpt = &options{
		FileTopic: "files", // default
	}
	fOpt        *file.Options
	producer, _ = bus.NewProducer(bus.NewOptions("nop"))
)

func main() {
	app := bootstrap.NewWorkerApp(taskType, newWorker, appOpt).
		Version(tools.String()).
		Description(description).
		FileOpts()
	app.Initialize()
	if appOpt.FileTopic != "-" {
		producer = app.NewProducer()
	}
	fOpt = app.GetFileOpts()
	app.Run()
}

type options struct {
	FileTopic string `toml:"file_topic" commented:"true" comment:"topic to publish written file stats"` // topic to publish information about written files
}

func (o *options) Validate() error { return nil }
