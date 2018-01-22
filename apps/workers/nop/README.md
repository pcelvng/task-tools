# Nop Worker 

The nop worker does not do anything except listen for tasks and mark the task
as a success or failure at random. The failure rate can be set at runtime. The
nop worker can simulate working on the task for a period of time by setting a 
task completion length or length range.

Build application with:

```bash
$ go build
```

For application run options use the -help
flag:

```bash
$ ./nop -help
```
