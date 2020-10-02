### File Watcher

- The filewatcher can create a file message in the files_topic in the bus
- it can also create a task message in the task_topic in the bus
- filewatcher options
  - `access_key` & `secret_key` (config option)
    - must be provided to access the S3 / GCS files
  - `files_topic` (config option)
    - a `stat.Stat` json object is sent to the files_topic
    - this can be disabled using a `-`
  - `task_topic` (config option)
    - a `task` is created and sent to the task_topic
    - the task info is created using the `task_template` in the RULE
    - if left empty a task is not created
  - `lookback` (rule option)
    - the number of hours to lookback for file changes
  - `frequency` (rule option)
    - the duration between times when the path is checked for new files
    - follows go duration standard `1h`, `1m`, `10s`, `100ms`
  - `path_template` (rule options)
    - the base path template to be searched for file changes
    - `tmpl.Parse` is used to parse the `path_template`
  - `task_template`
    - the template for the info string to send to the task_topic
    - should be a uri object
    - `origin[?query]`
    - uses `{WATCH_FILE}` to pass the found file name.
  - ```toml
	frequency = "1h"
	task_template = "{WATCH_FILE}?&param=data&dest=gs://dir/{HOUR_SLUG}/file.json"
	lookback = 24
	path_template = "gs://folder/{HOUR_SLUG}/*.json"
   ```

