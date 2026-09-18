# BigQuery design notes

These are the BigQuery concepts this project is meant to demonstrate
understanding of -- not all of them needed a full implementation to be
worth explaining and reasoning about.

## Columnar storage

BigQuery stores each column separately on disk rather than each row.
Every query in this project (DQ checks, fraud rules) reads a handful of
columns (`customer_id`, `amount`, `transaction_ts`, `country`,
`currency`, `status`) out of an eight-column table -- a columnar engine
only reads the bytes for those columns, not the whole row, which is both
faster and (since BigQuery's on-demand pricing bills per byte scanned)
directly cheaper.

## Serverless model

There's no cluster to size or manage -- BigQuery allocates compute
per-query automatically. For a workload like this one (a handful of
scheduled batch queries a day, not constant traffic) that avoids paying
for idle capacity, which a fixed-size warehouse would require.

## Partitioning by date

`transactions_raw` and `transactions_clean` are partitioned by
`DATE(transaction_ts)`. Because the DAG runs daily and every DQ/fraud
query in this project either filters by a date range or is naturally
scoped to "today's batch," partitioning means BigQuery can skip scanning
every partition outside that range entirely, rather than scanning the
whole table's history every run.

## Clustering by customer_id

Both tables are additionally clustered by `customer_id`. Every fraud
rule groups or self-joins by customer (velocity, amount-anomaly
baseline, geo-impossible-travel, repeated-failures) -- clustering
physically co-locates a customer's rows within each partition, so those
per-customer window functions and self-joins scan far fewer blocks than
they would against an unclustered table.

## How this scales beyond the current project

At 5,000 synthetic rows none of this matters for correctness -- it's
sized for a real-volume table (a payments issuer processes far more than
5,000 transactions/day). The partition + cluster design here is exactly
how this pipeline would keep working, and keep costing roughly the same
per query, if `transactions_raw` grew from thousands of rows to billions.

## A note on the free Sandbox tier

This project was actually run and validated against a real BigQuery
project using the free Sandbox tier (no billing account). One practical
limitation worth knowing: the Sandbox blocks DML statements
(`INSERT`/`UPDATE`/`DELETE`/`MERGE`) entirely -- only a billing-enabled
project can run those. `CREATE TABLE ... AS SELECT` (CTAS) is DDL, not
DML, and is allowed, so every script in `sql/` here is written using
CTAS instead of `INSERT INTO`, which is what let this project be
validated end-to-end without needing to enable billing.
