The sort2file worker reads from a source file
and sorts its records into 'hourly' destination files.

The worker assumes the source file contains json records.

Info Format:
"{source-file-path}?{querystring-params}" # for sorting a single file
"{source-dir-path}?{querystring-params}" # for sorting all files in a dir

Querystring Params:
* date-field (required: field name containing the date; for csv this is a field index
* date-format (go style date format)
* sep (csv separator; also indicates it is a csv type format)
* dest-template (required: destination template for sorted files)
* discard (true if should discard records that do not parse)
* use-file-buffer (true if files are too big to fig in memory and need to be buffered to file)

'dest-template' parameter:

Represents the full destination path and file name. Supports the following
template parameters:

- {YYYY}     four digit year ie 2007
- {YY}       two digit year ie 07
- {MM}       two digit month ie 01
- {DD}       two digit day ie 29
- {HH}       two digit hour ie 00
- {TS}       current timestamp (when processing starts) in following format: 20060102T150405
- {DAY_SLUG} shorthand for {YYYY}/{MM}/{DD}
- {SLUG}     shorthand for {YYYY}/{MM}/{DD}/{HH}
- {SRC_FILE} string value of the source file. Not the full path. Just the file name, including extensions.
- {SRC_TS}   source file timestamp (if available) in following format: 20060102T150405

A template '.gz' file extension will result in compressed destination files.

dest-template examples:

# gzipped output
?dest_template=s3://bucket-name/path/{YYYY}/{MM}/{DD}/{HH}/{HH}-{SRC_TS}.json.gz

# non-gzipped output
?dest_template=s3://bucket-name/path/{YYYY}/{MM}/{DD}/{HH}/{HH}-{TS}.json

# local file output (gzipped)
?dest_template=/local/path/{YYYY}/{MM}/{DD}/{HH}-{TS}.json.gz

'sep' parameter:

Common field separation values:

# comma (default)
?sep=,

# tab
?sep=\t

# pipe
?sep=|

'discard' parameter:

When true records that are missing the date field or do not parse correctly are
discarded.

When false, task processing will fail on the first record where:

- Record does not parse
- Number of fields is less than the date field index
- Date field does not parse

# discard turned off (default)
?discard=false

# discard turned on
?discard=true

result messages:

* 'complete' result

The task msg provides human readable statistics.

# typical
wrote 1000 lines over 3 files

# with discard option
wrote 900 lines over 3 files (100 discarded)

* 'error' result

Will provide approximately how many lines were processed
before the error and the error.

examples:
"issue at line 10: 'json parse error'"
"path 's3://bucket/path/to/file.txt' not found"

Example Tasks:
# json info
{
"type":"sort2file"
"info":"s3://bucket/file.json.gz?date-field=date&date-format=2006-01-02T15:04:05Z07:00&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/sorted-{SRC_TS}.json.gz&discard=true"
}

# csv info
{
"type":"sort2file"
"info":"s3://bucket/file.csv.gz?date-field=1&date-format=2006-01-02T15:04:05Z07:00&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.csv.gz&sep=,&discard=true"
}

# info from directory source path
{
"type":"sort2file"
"info":"s3://bucket/path/?date-field=1&date-format=2006-01-02T15:04:05Z07:00&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.csv.gz&sep=,&discard=true"
}

# minimal csv info
{
"type":"sort2file"
"info":"s3://bucket/path/file.csv?date-field=1&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.csv&sep=,"
}

# minimal json info
{
"type":"sort2file"
"info":"s3://bucket/path/file.json?date-field=date&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.json"
}

# minimal json info from directory source path
{
"type":"sort2file"
"info":"s3://bucket/path/?date-field=date&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.json"
}