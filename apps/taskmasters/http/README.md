# HTTP Taskmaster

http Taskmaster is used to manage tasks from the outside world using http request values. 

Note: query params overwrite json body values, if a body is provided it must be in json format
 
 ## Create a batch task
 - create a batcher task based on the http request values
 
 ``` curl localhost:port/batch?```
 
 ``` curl localhost:port/status?app=app_name```
 
 ``` curl localhost:port/job?command=fee-campaign&from=2018-10-01T00&to2018-11-01T00```

info - if provided it sends this value as the info value to the bus, if not provided will send the parsed request values in a uri string

**‼(PLEASE NOTE ~ underscores for json body request, dashes for uri query params)‼ 💣** 
### http request variables
- from - the start time of the first task to be created format RFC 3339 YYYY-MM-DDTHH:MM:SSZ (REQUIRED)
- *** pick a duration modifier ***
  - to - the end time of the last task to be created format RFC 3339 YYYY-MM-DDTHH:MM:SSZ (takes presidence over for value)
  - for - the duration that should be run starting at from (ignored if to value is provided)
- task_type     - the task type for the new tasks (REQUIRED)
- every-x-hours - will generate a task every x hours. Includes the first hour. Can be combined with 'on-hours' and 'off-hours' options.
- on-hours      - comma separated list of hours to indicate which hours of a day to back-load during a 24 period (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. Example: '0,4,15' - will only generate tasks on hours 0, 4 and 15
- off-hours     - comma separated list of hours to indicate which hours of a day to NOT create a task (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. If used will trump 'on-hours' values. Example: '2,9,16' - will generate tasks for all hours except 2, 9 and 16.
- topic         - overrides task-type as the default topic
- fragment      - task destination template (may have to build a registry for these)

Examples:
```bash
curl -v -X POST -d '{"task-type":"batcher","every-x-hours":"1","from":"2018-05-01T00:00:00Z"}' 'localhost:{http_port}/path/is/ignored/'
curl -v -X GET 'localhost:{http_port}/path/is/ignored/?task-type=example-task&from=2018-05-01T00:00:00Z'
```