### File Watcher
   
- The s3-watch creates a file when a new file has been detected
- by default the check happens for the past 24 hours

##### Option Params:
- lookback - the amount of time to lookback for file changes (for each directory)
- dir_template - the base path template to be searched for file changes uses a year/month/day/hour file template lookup
- aws_access_key & aws_secret_key must be provided to access the S3 bucket


