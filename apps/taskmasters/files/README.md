# Files Taskmaster

Listens on a message bus for file stats json objects.

# Info

```bash
# immediate response mode
"s3://bucket/path/to/obj/file.txt.gz?size={size}&checksum={checksum}&created={created}&lines={lineCnt}"

# dir mode - the source path is a directory representing one or more files created that the worker can 
# operate from.
"s3://bucket/path/to/obj/"

```

# Toml Rules

## Toml Fields
```toml
bus = "" #
```


```toml
[[rule]]
type = "sortbyhour"
src_pattern = "s3://rmd-partners/facebook/raw-hourly/*/*/*/*/"

# if cron_check is specified, then files that match src_pattern are sequestered until
# the croncheck triggers a check that will create a task for every pattern match
cron_check = "* * * * *"

[[rule]]
type = "sortbyhour"
src_pattern = "s3://rmd-partners/facebook/raw-hourly/{yyyy}/{mm}/{dd}/{hh}/"

# If count_check is specified then task creation will occur after
# count_check number of files has been received that match src_pattern.
# A task will be created for every unique directory in the file group. 
count_check = 100 

[[rule]]
type = "dedup"
src_pattern = "s3://rmd-partners/facebook/hourly/raw-sorted/{yyyy}/{mm}/{dd}/{hh}/"
mode = "immediate_response" # default

[[rule]]
type = "fb-hourly-transform"
src_pattern = "s3://rmd-partners/facebook/hourly/raw-deduped/{yyyy}/{mm}/{dd}/{hh}/"

[[rule]]
type = "fb-hourly-load"
src_pattern = "s3://rmd-partners/facebook/hourly/processed/{yyyy}/{mm}/{dd}/{hh}/"

[[rule]]
type = "fb-hourly-agg-load"
src_pattern = "s3://rmd-partners/facebook/hourly/processed/{yyyy}/{mm}/{dd}/{hh}/"
```

# Two Modes

1. Respond immediately on file creation
2. Check file 'groups' periodically
	- every hour on a minute offset