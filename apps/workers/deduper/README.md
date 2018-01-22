# Dedup Worker

Deduper uniques lines of a JSON file based on a set of unique fields and timestamp.

## Info Definition
worker information should be formatted like a url

  * ReadPath - Definition of file to read defined by a URI origin (scheme, host, path)
  * WritePath - Definition of file to be written to
  * Key - An array of JSON keys used to uniquely identify the record
  * TimeField - the JSON key that is holding the time field (RFC3339 format "2006-01-02T15:04:05Z07:00")
  * Keep - identifies which duplicate record to keep
    * Newest - record with most recent timestamp (default)
    * Oldest - record with oldest timestamp
    * First - First record (never override)
    * Last - Last record (always override)

### Example

``` golang
 s3://bucket/path/to/file.json?Key=field1,field2&TimeField=dt&Keep=Newest&WritePath=/usr/bin/output.json

 // ReadPath = "s3://bucket/path/to/file.json"
 // WritePath = "/usr/bin/output.json"
 // Key = []string{"field1","field2"}
 // TimeField = "dt"
 // Keep = "newest"
```


## Config Definition

