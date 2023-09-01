# task-tools
[![CircleCI](https://circleci.com/gh/pcelvng/task-tools/tree/master.svg?style=svg)](https://circleci.com/gh/pcelvng/task-tools/tree/master)
A set of tools and apps used in the [task](https://github.com/pcelvng/task) ecosystem

## Getting Started 


### bootstrap

## Useful Apps

### flowlord
Flowlord is the recommended all-purpose TaskMaster that should be used with workflow files to schedule when tasks should run and the task hierarchy. It can retry failed jobs, alert when tasks fail and has an API that can be used to backload/schedule jobs and give a recap of recent jobs run. 

See Additional [docs](apps/taskmasters/flowlord/README.md).  

### Workers
- bq-load: BigQuery Loader 
- sql-load: Postgres/MySQL Optimized loader 
- sql-readx: Postgres/MySQL reader with ability to execute admin query
- db-check: Monitoring tools to verify data is being populated as expect in DB
- transform: generic json modification worker that works uses gojq


## Utilities 

### File Tools 

- local reader/writer
- s3 reader/writer
- copy
- general reader initializer; will choose correct reader based on file path
- general writer initializer; will choose correct writer based on path
- globbing; multiple readers/writers from a glob pattern

### Slack 
Utility to send messages to slack. 

``` go 
func main() {
    notify := slack.Slack{
        Url: "https://hooks.slack.com/services/ORG_ID/APP_IP/CHANNEL_ID
    }
    notify.Notify("Hello World", slack.OK) 
}
```