# task-tools
Tools to enhance the Task experience


# v0.1.0 Todo List

**Taskmasters:**

- 'backloader'; create batch of tasks backloading/running over a long period of time (in progress)
- 'cron'; create tasks in response to the passage of time (in progress)
- 'http'; will create tasks from an http rest call
- 'complete'; will create tasks based on completed tasks
- 'retry'; will retry failed tasks (in progress)
- 'dir'; will watch a dir and create tasks when new files are created, will support local files and s3
- 'file'; will listen on a topic for 'file' json objects and create tasks in response
- 'db'; will listen on a topic for 'db' json objects and create tasks in response
- 'audit'; will listen on a topic for 'audit' json objects and create tasks in response

**Workers:**

- 'noop' (in progress)
- 'sort-by-hour'; read in a file and write to multiple files sorted by a date field (in progress)
- 'copy'


**Auditors:**

- 'dir'; will check that files exist in a dir
- 'file'; will audit a created file
- 'db'; will audit db record counts

**File Tools**

- local reader/writer
- s3 reader/writer
- copy
- general reader initializer; will choose correct reader based on file path
- general writer initializer; will choose correct writer based on path
- globbing; multiple readers/writers from a glob pattern

**Utility Apps:**

- log cat utility
- log tail utility
- log stats utility


**Other:**

- distributed logging
- distributed logging: Statsd
- distributed logging: Prometheus
- distributed logging: InfluxData ???

