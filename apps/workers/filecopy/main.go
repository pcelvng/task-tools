package main

import (
	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
)

type options struct {
	FileTopic    string        `toml:"file_topic" commented:"true" comment:"topic to publish written file stats"` // topic to publish information about written files
	ReadOptions  *file.Options `toml:"read_options"`
	WriteOptions *file.Options `toml:"write_options"`
}

var (
	taskType    = "filecopy"
	description = `filecopy is a simple worker to copy a file from one location to another, local, or remotely (s3)

## Info Definition
worker info string uses a url type format:

"{src-path}?{querystring-params}"

Where:
  * src-path - is a file path to copy to another location.
  
  * querystring-params can be:
  - dest-template - copied file destination and supports the following template tags:
    {SRC_FILE} (string value of the source file. Not the full path. Just the file name, including extensions.)
    {YYYY}     (year - four digits: ie 2017)
    {YY}       (year - two digits: ie 17)
    {MM}       (month - two digits: ie 12)
    {DD}       (day - two digits: ie 13)
    {HH}       (hour - two digits: ie 00)
    {TS}       (timestamp in the format 20060102T150405)
    {SLUG}     (alias of HOUR_SLUG)
    {HOUR_SLUG} (date hour slug, shorthand for {YYYY}/{MM}/{DD}/{HH})
    {DAY_SLUG} (date day slug, shorthand for {YYYY}/{MM}/{DD})
    {MONTH_SLUG} (date month slug, shorthand for {YYYY}/{MM})

    - use-file-buffer - set to 'true' if file processing should use a file buffer instead of memory
    - note: the worker app user must have permissions to read and write to this locaiton

Example task:
 
{"type":"file-copy","info":"s3://bucket/path/to/file.json?dest-template=s3://bucket/to/destination/location/{SRC_FILE}"}
{"type":"file-copy","info":"/path/to/source.json?dest-template=/path/to/destination/location/output.json"}
 
 Query string params:
 - dest-template 
`

	fOpt        *file.Options
	producer, _ = bus.NewProducer(bus.NewOptions("nop"))
)

func main() {
	appOpt := &options{
		FileTopic: "files", // default
	}

	app := bootstrap.NewWorkerApp(taskType, appOpt.newWorker, appOpt).
		Version(tools.String()).
		Description(description)
	app.Initialize()
	if appOpt.FileTopic != "-" {
		producer = app.NewProducer()
	}
	fOpt = app.GetFileOpts()
	app.Run()
}

func (o *options) Validate() error { return nil }
