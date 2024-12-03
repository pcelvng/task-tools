# bq-load 
Load line deliminated json files into BigQuery or execute SQL queries

[Google Cloud Docs](https://cloud.google.com/bigquery/docs/loading-data-local)

## Info Params 

- `origin`: [required] file to be inserted (gs://path/file.json) or SQL query to be run (.sql)
- Destination: (at least one)  
  - `dest_table`: [required for load] project.dataset.table to be inserted into 
  - `dest_path`: string; export query results to a file. Will automatically export to GCS if a star is in the path
- `direct_load`: bool; fastest why to load but the bucket must be in the same project
- pre-insert options: [optional] table cleanup to prevent duplicates
  - `truncate`: delete everything and insert
  - `delete`: create a delete statement based the column matches the values passed in the map (delete=id:10|date:2020-01-02)
- `params`: query parameters in format param=value (supports string, int, float, bool, and date YYYY-MM-DD format)
- `interactive`: bool; makes queries run faster for local development

  
## Supported File Format
- .json - Load JSON data into BigQuery
- .csv - Load CSV data into BigQuery
- .sql - Execute SQL queries with optional parameters

## Examples task
{"task":"bq_load", "info":"gs://my-bucket/data.json?dest_table=project.reports.impressions&delete=date:2020-01-02|id:11&direct_load"}
{"task":"bq_load", "info":"./data/*.json?dest_table=project.reports.impressions"}
{"task":"bq_load", "info":"query.sql?dest_table=project.dataset.table&params=date:2023-01-01|user_id:123|active:true"}
{"task":"bq_load", "info":"query.sql?interactive=true&dest_path=gs://my-bucket/results-*.csv"}