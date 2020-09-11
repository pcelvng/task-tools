# bq-load 
takes one of more line deliminated json files and loads them into a bigquery table. 

[Google Cloud Docs](https://cloud.google.com/bigquery/docs/loading-data-local)

## Task info string 

- origin: (gs://path/file.json) - location of file to be uploaded (s3, gs or local)
- *Insertion Option* - 1 of 3
  - truncate: truncate the table before insertion 
  - append: (default) append the file to the table
  - delete: a map of key, values used to delete data before insertion (delete=date:2020-01-02)