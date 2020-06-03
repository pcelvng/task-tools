# Stats

The stats utility consumes all all tasks on the task 
(that transmit task objects). The stats utility can then be queried via 
simple REST calls for general information about the active task ecosystem, In additional this information is available on a Prometheus endpoint.

## Statistics 
- Average time to complete a task of a particular type
- Error rates (across all tasks types and by task type)
- Error totals (across all task types and by task type)
- Average number of tasks processed per hour (broken down by task type)
- Total tasks completed (broken down by total completed and total error)
- Uptime of the stats utility (since stats state is not recorded anywhere)
- Tasks that were created but don't have a corresponding 'done' record

## Prometheus
- Number of successful tasks (broken down by task type and job id)
- Number of failed tasks (broken down by task type and job id)
- Runtime of tasks (broken down by task type and job id)
