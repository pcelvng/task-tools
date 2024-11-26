# bq-load 
load line deliminated json files into BigQuery

[Google Cloud Docs](https://cloud.google.com/bigquery/docs/loading-data-local)

## Info Params 

- origin: [required] file to be inserted (gs://path/file.json)
- dest_table: [required] project.dataset.table to be inserted into
  - direct_load: bool; Its fastest to load directly for GCS, but the bucket must be in the same project.
- Optional pre-insert table cleanup to prevent duplicates
  - `truncate`: delete everything and insert
  - `delete`: create a delete statement based the column matches the values passed in the map (delete=id:10|date:2020-01-02)

## Examples task
{"task":"bq_load", "info":"gs://my-bucket/data.json?dest_table=project.reports.impressions&delete=date:2020-01-02|id:11&direct_load"}
{"task":"bq_load", "info":"./data/*.json?dest_table=project.reports.impressions&from_gcs=true&append=true"}`