<h1>sql-load</h1> 
<h3>A generic worker to load a newline delimited json (ndj) into a sql databse.</h3> 
<h3>Initially only postgresql will be supported.</h3>.

* info query params:
  * `origin` : (required) file, or folder path (all files) to parse and insert into `table`
  * `table` : (required), the name of the table to be inserted into i.e., schema.table_name
  * `delete` : will build and run a delete statement based on provided params
    * this is executed before the insert statements
    * provide a list of delete key:values to be used in the delete statement
    * `"?delete=date:2020-07-01|id:7"`
  * `truncate`: will truncate (delete all) from the `table` before the insert
  * `fields` : allows mapping different json key values to different database column names
    * provide a list of field name mapping {json key name}:{DB column name} to be mapped 
    * `?fields=jsonKeyName:dbColumnName`
  * `cached_insert` : will create a temp table for insert
    * this temp table data will then be inserted into the [`table_name`]

Example tasks:

```json 
// These will use the sql batch loader
{"type":"sql_load","info":"gs://bucket/path/to/file.json?table=schema.table_name&delete=date:2020-07-01|id:7"}

{"type":"sql_load","info":"gs://bucket/path/of/files/to/load/?table=schema.table_name"}

{"type":"sql_load","info":"gs://bucket/path/to/file.json?table=schema.table_name&delete=date:2020-07-01|id:7&fields=jsonKeyValue:dbColumnName"}

// These examples will use the cached_insert 
// this creates a temp table to insert the data, 
// and inserts into the main table from the temp table to improve insert loading time
{"type":"sql_load","info":"gs://bucket/path/to/file.json?cached_insert&table=schema.table_name&delete=date:2020-07-01|id:7&fields=jsonKeyValue:dbColumnName"}

{"type":"sql_load","info":"gs://bucket/path/to/file.json?cached_insert&table=schema.table_name&truncate&fields=jsonKeyValue:dbColumnName"}
```