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

## Main query

### Order by clause
Internally, the extractor uses a LIMIT and OFFSET clause to control how much data is pulled at a time.
For this reason, it's important that the main query contains an ORDER BY clause and that the order by 
clause produces consistent ordering. See Postgres documentation on the use of the LIMIT and OFFSET clause.

### Example query

```sql
SELECT t.TxType as "Type",t.AccountID as "AccountID", t.Ticker as "Ticker"
               to_char(t.TDate,'YYYY-MM-DD HH24:MI:SS') as "TDate",
               to_char(t.SDate,'YYYY-MM-DD HH24:MI:SS') as "SDate",
               t.Amt as "Amt",
               t.Ccy as "Ccy",
               t.Qty as "Qty",
               t.Commission as "Commission"
               t.TxID as "TxID"       
        FROM transactions t
                WHERE  lastmod >= $1 AND lastmod < $3'"
                 ORDER BY t.TxID 
```

The ```"``` are used to preserve the case in the output.

### Additional filters
#### Time filters
Avoid using additional time filters. Internally, the next last-read time is grabbed before processing
begins using a SELECT MAX(lastmod) FROM ... query. If you use additional time filters, there's a risk 
the query will not get all data, but the last read will be set as if all data has been read.

#### Other filters
If you use filters based on data that changes, make sure any changes are applied before extracting data.


