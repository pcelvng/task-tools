# task-tools
[![CircleCI](https://dl.circleci.com/status-badge/img/gh/pcelvng/task-tools/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/pcelvng/task-tools/tree/main)

A set of tools and apps used in the [task](https://github.com/pcelvng/task) ecosystem

## Getting Started 

### Creating a worker 

``` go
const desc = "cli desciption"  
type options struct {
	// worker specific config values 
}

func main() {
	opts := &options{}
	app := bootstrap.NewWorkerApp("app Name", opts.NewWorker, opts).
		Description(desc).
		Version(tools.Version).Initialize()

	app.Run()
}

func (o *options) NewWorker(info string) task.Worker {
  // TODO: parse the info string and setup a new Worker
  return worker{
    Meta:    task.NewMeta(),
  } 
}

type worker struct {
	task.Meta
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
  // TODO: Process the requested job and return Complete, error and details about the job. 
  return task.Completed("All done")
}

```

### Creating a taskmaster 

``` go 
desc = "cli description"
func main() {
    opts := &options{}
	app := bootstrap.NewTaskMaster("appName", NewFunc, opts).
		Version(tools.String()).
		Description(desc)
	app.Initialize()
	app.Run()
}

func New(app *bootstrap.TaskMaster) bootstrap.Runner {
	return &taskMaster{}
}

func (tm *taskMaster) Info() interface{} {
	// provide a struct of data to be display on the /info status endpoint
	return struct{}
}

func (tm *taskMaster) Run(ctx context.Context) error {
	// main running process
	// read from consumer and produce tasks as required
}

```

## Pre-built Apps 

### **Flowlord**
an all-purpose TaskMaster that should be used with workflow files to schedule when tasks should run and the task hierarchy. It can retry failed jobs, alert when tasks fail and has an API that can be used to backload/schedule jobs and give a recap of recent jobs run. 

See Additional [docs](apps/taskmasters/flowlord/README.md).  


### Workers
- **bq-load**: BigQuery Loader
- **sql-load**: Postgres/MySQL Optimized Idempotent loader
- **sql-readx**: Postgres/MySQL reader with ability to execute admin query
  - perfect for creating schedule partitions or other schedule admin tasks
- **db-check**: Monitoring tools to verify data is being populated as expect in DB
- **transform**: generic json modification worker that works uses gojq


## Utilities 

### File Tools 
read/write from local, s3, gcs, minio with the same tool. Use a URL to distinguish between the providers. 
  - `s3://bucket/folder`
  - `gs://bucket/folder`
  - `mc://bucket/folder`
  - `local/folder/`

``` go
opts :=  &file.Options{ AccessKey: "123", SecretKey: "secret123"}
```
#### list details about all files with a remote s3 directory
``` go
for _, f := range file.List("s3://bucket/folder/", opts) {
  fmt.Println(f.JSONString())
} 
```
#### read and process data from a file 
``` go 
  reader, err := file.NewReader("gs://bucket/folder/file.txt", opts)
  if err != nil {
    log.Fatal(err) 
  }
  scanner := file.NewScanner(reader) 
  for scanner.Scan() { // go through each line of the file
    fmt.Println(scanner.Text()) 
    // process data 
  }
```

#### write to a file

``` go
writer, err := file.NewWriter("s3://bucket/folder/data.txt", opts) 
data = []any{} // some sort of data 
for _, d := range data {
  b, _ := json.Marshal(data) 
  writer.WriteLine(b) // vs writer.Write(b) 
}
if fatalError {
  // don't commit the file and cancel everything 
  writer.Abort()
  return 
}

writer.Close()  
```

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