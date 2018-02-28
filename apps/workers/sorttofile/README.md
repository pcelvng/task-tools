# sorttofile Worker

The sorttofile worker reads from a source file and sorts its records into 'hourly'
destination files.

The worker assumes the source file contains json records. 

## task type

```
# task type
sorttofile
```

## task info

The task info field is in the following format:

```
# info 
{source-file-path}?{querystring_params}
```

Example:

```
# example json info
s3://bucket/file.json.gz?date-field=date&date-format=2006-01-02T15:04:05Z07:00&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/sorted-{SRC_TS}.json.gz&discard=true

# example csv info
s3://bucket/file.csv.gz?date-field=1&date-format=2006-01-02T15:04:05Z07:00&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.csv.gz&sep=,&discard=true

# example info from directory source path
s3://bucket/path/?date-field=1&date-format=2006-01-02T15:04:05Z07:00&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.csv.gz&sep=,&discard=true

# example minimal csv info
s3://bucket/path/file.csv?date-field=1&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.csv&sep=,

# example minimal json info
s3://bucket/path/file.json?date-field=date&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.json

# example minimal json info from directory source path
s3://bucket/path/?date-field=date&dest-template=s3://bucket/dir/{YYYY}/{MM}/{DD}/{HH}/{SRC_TS}.json
```

### date-field (required)

Represents json date field.

```
# "createdAt" contains the date to sort on
?date-field=createdAt
```

### date-field-index (required)

Represents the zero-offset date field index. Must be a positive integer.

```
# first field contains the date
?date-field=0

# tenth field contains the date
?date-field=9
```

### date-format

Takes a golang standard time.Time string format. See: https://golang.org/pkg/time/#pkg-constants

```
# default (time.RFC3339)
?date-format=2006-01-02T15:04:05Z07:00
```

### dest-template (required)

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

Examples:

```
# gzipped output
?dest_template=s3://bucket-name/path/{YYYY}/{MM}/{DD}/{HH}/{HH}-{SRC_TS}.json.gz

# non-gzipped output
?dest_template=s3://bucket-name/path/{YYYY}/{MM}/{DD}/{HH}/{HH}-{TS}.json 

# local file output (gzipped)
?dest_template=/local/path/{YYYY}/{MM}/{DD}/{HH}-{TS}.json.gz 
```

### sep

Common field separation values:

```
# comma (default)
?sep=,

# tab
?sep=\t

# pipe
?sep=|
```

### discard

When true records that are missing the date field or do not parse correctly are
discarded. 

When false, task processing will fail on the first record where:

- Record does not parse
- Number of fields is less than the date field index
- Date field does not parse

```
# discard turned off (default)
?discard=false

# discard turned on
?discard=true
```

## task msg

### complete result

The task msg provides human readable statistics.

Examples:

```
# typical
wrote 1000 lines over 3 files

# with discard option
wrote 900 lines over 3 files (100 discarded) 
```

### error result

Will provide approximately how many lines were processed 
before the error and the error.

Example:

```
issue at line 10: 'json parse error'
```

If no records were processed then will just provide the
error.

Example:

```
path 's3://bucket/path/to/file.txt' not found
```

