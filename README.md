# sql-extract-modified
Very basic sql extractor to get data modified since last extraction

Currently postgres specific due to the ON CONFLICT clause

## Last read table
This is used to store the last read time. 

It should look like:
__________________________________
| tablename | text primary key   |
----------------------------------
| lastread  | timestamp not null |
----------------------------------