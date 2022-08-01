# sql-extract-modified
Very basic sql extractor to get data modified since last extraction. Note: it may get duplicate
data.

Currently postgres specific due to the ON CONFLICT clause

## Last read table
This is used to store the last read time. It should be created automatically.

It should look like:
__________________________________
| tablename | text primary key   |
----------------------------------
| lastread  | timestamp not null |
----------------------------------

## Good before
This is a why to indicate that data before this time is up to date. Then if a process runs to 
push more data, whilst this process is reading data, it should prevent this process picking up partial
data. 

Example table:
---------------------------------
| table: data_status             |
---------------------------------
| tablename   | text primary key |
---------------------------------
| good_before | timestamp        |
---------------------------------

And an example query:
```sql
SELECT good_before FROM data_status WHERE tablename='my_data_table'
```

If a good_before query is specified, it must be passed to the main query in the parameter $3 



