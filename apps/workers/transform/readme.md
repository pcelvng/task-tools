# Transform

a generic worker that provides a means to process json logs. Transform uses gojq internal to modify json and will behave the same as if running jq from the command line. 

Transform is different than running on the command line as it has support for reading and writing to remote services and is easily scheduled and managed using _task_.

## Config 

  - File: used to configure access for reading and writing

### Info string 
`gs://path/to/file/*/*.gz?dest=gs://path/dest/output.gz&jq=./conf.jq`

 - origin: `gs://path/to/file/*/*.gz` - file(s) to process
 - dest: `dest=gs://path/dest/output.gz` - destination path for output
 - jq: `jq=./conf.jq` - jq definition file
- Threads: number of threads to use process the logs, increase to utilize more CPUs

## Performance
Transform performs a bit slower than jq single threaded, but runs much better with multiple threads.

basic : 115k lines test 
- jq: 53s 
- gojq: 28s (v0.12.0)
- transform 1 thread: 40s 
- transform 2 threads: 25s