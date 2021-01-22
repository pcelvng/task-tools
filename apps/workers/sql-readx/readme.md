# sql-readx
a generic sql worker used to query a database or execute statements. 

## config 
`./sql-readx -g toml` generate a default config 

## info params 

- **origin**: (alternative to query) - path to a file containing a sql statement
- **query**: (instead of file) - statement to execute
- **exec**: execute statement instead of running as a query
- **dest**: (required for query) - file path to where the file should be written
- **table**: table (schema.table) to read from if
- **field**: map of columns of fields.
  - query: list of columns to read from and the json field that should be used to write the values.
  - exec: key to be replaced with value in statment. NOTE: key are wrapped with brackets {key} -> value

### Examples

- generated query based on url table and field values 
  - `{"task":"sql_readx","info":"?dest=./data.json&table=report.impressions&field=id:my_id|date:date"}`
- query from a file
  - `{"task":"sql_readx","info":"./query.sql?dest=./data.json"}`
- exec a command from a file with named query params
  - `{"task":"sql_readx","info":"./query.sql?exec&field=date:2020-01-01"}`
