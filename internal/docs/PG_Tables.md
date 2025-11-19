# Postgres Tables 
Suggested tables to be able to track and store processed task records. 

### Required Functions 

``` sql 
-- parse_param will convert a url param without a starting ? to a json key,value pair 
CREATE OR REPLACE FUNCTION parse_param(url text)
RETURNS jsonb AS $$
    DECLARE
    result    jsonb := '{}';
    param     text;
    key       text;
    value     text;
BEGIN
   FOREACH param IN ARRAY regexp_split_to_array(url, '&')
        LOOP
            key := split_part(param, '=', 1);
            value := coalesce(split_part(param, '=', 2),'');
            if key is not null and key != '' then
            result := jsonb_set(result, ARRAY [key], to_jsonb(value), true);
            end if;
        END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- parse_url takes a url value, extract the origin and the query params into a json object
CREATE OR REPLACE FUNCTION parse_url(url text)
RETURNS jsonb AS $$
DECLARE
    result    jsonb := '{}';
    query     text;
    origin    text;
BEGIN
    -- Construct origin
    origin := coalesce(substring(url from '^((?:([^:/?#]+)://([^/?#]*))?([^?#]*))'), '');
    result := jsonb_set(result, '{origin}', to_jsonb(origin), true);

    -- Extract query parameters
    query := substring(url from '\?.*$');
    IF query IS NOT NULL THEN
        query := substring(query from 2); -- Remove the leading '?'
        result := result || parse_param(query);
    END IF;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
```

## Task-logs
used to store the done tasks. 

``` sql 
create table public.task_log
(
    id      text,
    type    text,
    job     text,
    info    text,
    result  text,
    meta    text,
    msg     text,
    created timestamp,
    started timestamp,
    ended   timestamp
);

create index task_log_created
    on public.task_log (created);

create index task_log_started
    on public.task_log (started);

create index task_log_type
    on public.task_log (type);

create index task_log_job
    on public.task_log (job);
```

## Task View 
a user friendly view of task-logs that adds time to complete task in _**task_seconds**_ and _**task_time**_ and parsed url values of _**info**_ and _**meta**_ 

``` sql 
create or replace view public.tasks (id, type, job, info, infoJSON, meta, metaJSON, msg, result, task_seconds, task_time, created, started, ended) as
select task_log.id,
       task_log.type,
       task_log.job,
       task_log.info,
       parse_url(task_log.info),
       task_log.meta,
       parse_param(task_log.meta),
       task_log.msg,
       task_log.result,
       date_part('epoch'::text, task_log.ended - task_log.started)    as task_seconds,
       to_char(task_log.ended - task_log.started, 'HH24:MI:SS'::text) as task_time,
       task_log.created,
       task_log.started,
       task_log.ended
from task_log;
```