### File Watcher
   
- The file-watcher creates a file on the bus when a new file has been detected in the templated path
- by default the check happens for the past 24 hours each hour

##### Option Params:
- topic
  - default files topic is 'files' but can be over written
- lookback
  - the amount of time to check back for file changes (for each directory)
- path_template
  - the base path template to be searched for file changes uses a year/month/day/hour file template lookup
- aws_access_key & aws_secret_key
  - must be provided to access the S3 bucket
- frequency
  - the duration between times when the path is checked for new files

