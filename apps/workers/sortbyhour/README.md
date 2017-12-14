# Sort By Hour Worker

The sortbyhour worker accepts a file location in the task info and sorts the records into 'hourly'
destination files. 

The worker supports JSON and CSV formats. 

The date field to sort on is also specified in the info string. If the file format is a 
CSV then the date field is provided as a column number. If the input file format is JSON then
the date field to sort is the name of the JSON field. The sorter will not parse out entire JSON
records but will look for any JSON field with that name. Therefore, if the field name is repeated then 
only the first instance value of that field will be used in sorting.

If the field is not found then the worker will stop working on the task and return the task with an
'error' result and a 'date field not found' msg.

If the field value is not time parsable then the worker will fail out the task with an 'error' result 
and a 'date field not parsable' msg.

CSV files have another option to specify the separator value. The default separator is ','.

sortbyhour is expecting a uri-like format as follows:

```
'{file-location}?{querystring parameters}'
```

File Location

Full location of the source file. If it begins with 's3://' then the sorter will 
know it is an s3 source location. Otherwise the sorter will assume the source file
comes from a local file location. If the source file name ends in '.gz' then the 
sorter will assume the file is gzipped.

Supported Query-string parameters

```
date-field={field-name}
date-field-format={format} # uses golang date formatting # default: 2006-01-02T15:04:05Z07:00
sorted-file-template={template} # see below on usage
separator={separator-value}
```

Sample info string for a JSON file input:

```
'./file.json?date-field=date_created'
```

Sample info string for a CSV file input:

```
'./file.csv?date-field=3'
```

CSV with tab separation:

```
'./file.csv?date-field=3&separator=\t'
```

### sorted-file-template

Represents the full destination path and file name. Supports the following
template parameters:

- {yyyy}   four digit year ie 2007
- {mm}     two digit month ie 01
- {dd}     two digit day ie 29
- {hh}     two digit hour ie 00
- {host}   host name of sort-by-hour application
- {cur_ts} current timestamp (when processing starts) in following format: 20060102T150405

If the sorted-file-template ends in '.gz' the output files will be compressed.

Examples:

s3://bucket-name/path/{yyyy}/{mm}/{dd}/{hh}/{hh}-{cur_ts}.json.gz # gzipped output
s3://bucket-name/path/{yyyy}/{mm}/{dd}/{hh}/{hh}-{cur_ts}.json # non-gzipped output
/local/path/{yyyy}/{mm}/{dd}/{hh}-{cur_ts}.json.gz # local file output
