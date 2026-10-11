# 10 — Athena

> [Index](README.md) · [← 09 — RDS (Postgres)](09-rds-postgres.md) · [11 — Kinesis Data Streams →](11-kinesis-data-streams.md)

Run SQL over S3 data through tables registered in the Glue Data Catalog.

---

## 1. Documentation

### Overview

- **Workgroup**: holds the query result location and settings.
- **Query**: `start_query_execution` → poll `get_query_execution` → read `get_query_results`.
- Tables come from the **Glue Data Catalog** ([guide 03](03-glue.md)).

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `athena` |
| Terraform endpoint | `athena` |
| Needs first | a results bucket ([01](01-s3.md)) and Glue tables ([03](03-glue.md)) |
| Docker socket | **required** (the `floci-duck` sidecar is pulled on the first query) |

### How it behaves on Floci

Floci runs **real SQL** through a `floci-duck` (DuckDB) sidecar container (pulled on first query, so Docker is required):

- Every Glue database becomes a DuckDB schema; every Glue table becomes a view over its S3 location. Format is inferred from the table's `InputFormat`/SerDe (`parquet`, `json`, otherwise CSV).
- Tables in the query's context database also work **unqualified** (`FROM orders_clean`).
- **Register tables through the Glue API.** Athena DDL such as `CREATE EXTERNAL TABLE` is not how Floci learns tables (`sql/athena/create_orders_ext.sql` is for real AWS only).
- Results are CSV files in the workgroup output location.
- `FLOCI_SERVICES_ATHENA_MOCK=true` makes queries succeed with empty results (state-machine tests only).

### Gotchas

- Every value comes back as a string (`VarCharValue`); cast in Python.
- The first row of the first result page is the header; `fetch` skips it.
- A table `Location` must be a folder prefix ending in `/`.
- On failure `run()` raises with Athena's `StateChangeReason`.

---

## 2. Basic Usage

> Prerequisite: Floci is running with the Docker socket mounted, and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

### Run a query

```python
import time
import boto3
from botocore.config import Config

s3 = boto3.client("s3", config=Config(s3={"addressing_style": "path"}))
athena = boto3.client("athena")

s3.create_bucket(Bucket="demo-athena-results")

qid = athena.start_query_execution(
    QueryString="SELECT 1 AS n",
    ResultConfiguration={"OutputLocation": "s3://demo-athena-results/results/"})["QueryExecutionId"]

while True:                                     # poll until the query finishes
    status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
    if status["State"] not in ("QUEUED", "RUNNING"):
        break
    time.sleep(1)
print(status["State"])                          # SUCCEEDED

rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
print([[c.get("VarCharValue") for c in r["Data"]] for r in rows])   # [['n'], ['1']]  (first row = header)
```

### Query a Glue table

```python
# (needs Floci and a table registered through Glue, see guide 03)
qid = athena.start_query_execution(
    QueryString="SELECT count(*) AS n FROM orders_clean",
    QueryExecutionContext={"Database": "floci_demo_db"},
    ResultConfiguration={"OutputLocation": "s3://demo-athena-results/results/"})["QueryExecutionId"]
```

---

## 3. Examples

### Example 1 — Workgroup and table bootstrap

Databases and tables are registered through Glue, not Athena DDL.

`src/athena_ops/setup.py`

```python
from botocore.exceptions import ClientError

from src.common.aws import client, name
from src.glue_ops import catalog

athena = client("athena")
DB = "floci_demo_db"


def ensure_workgroup(wg: str, results_bucket: str) -> None:
    try:
        athena.create_work_group(
            Name=wg,
            Configuration={
                "ResultConfiguration": {"OutputLocation": f"s3://{results_bucket}/results/"},
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": False,
            })
    except ClientError as e:
        if "already" not in e.response["Error"]["Message"].lower():
            raise


def bootstrap(wg: str | None = None) -> str:
    """
    Databases and tables come from the Glue catalog (Floci's Athena reads them from there).
    Athena DDL such as CREATE EXTERNAL TABLE is NOT what Floci uses, so register via Glue.
    """
    wg = wg or name("wg")
    ensure_workgroup(wg, name("athena-results"))
    catalog.ensure_database(DB)
    catalog.ensure_csv_table(DB, "orders_raw", f"s3://{name('raw')}/raw/orders/", catalog.ORDERS_COLUMNS)
    catalog.ensure_csv_table(DB, "orders_clean", f"s3://{name('curated')}/clean/orders/",
                             catalog.ORDERS_COLUMNS)
    return wg
```

### Example 2 — Query helpers (run, fail reason, fetch, DataFrame)

`src/athena_ops/query.py`

```python
from src.common.aws import client
from src.common.waiters import wait_until

athena = client("athena")


def run(sql: str, workgroup: str = "primary", database: str | None = None, timeout: int = 300) -> str:
    """Start a query, wait for it, return the QueryExecutionId. Raises with Athena's reason on failure."""
    kwargs = dict(QueryString=sql, WorkGroup=workgroup)
    if database:
        kwargs["QueryExecutionContext"] = {"Database": database}
    qid = athena.start_query_execution(**kwargs)["QueryExecutionId"]
    try:
        wait_until(lambda: athena.get_query_execution(QueryExecutionId=qid)
                   ["QueryExecution"]["Status"]["State"],
                   done={"SUCCEEDED"}, failed={"FAILED", "CANCELLED"}, timeout=timeout, interval=1)
    except RuntimeError as exc:
        raise RuntimeError(f"{exc}: {explain_failure(qid)}") from exc
    return qid


def explain_failure(qid: str) -> str:
    return athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"].get(
        "StateChangeReason", "")


def fetch(qid: str) -> list[dict]:
    """All rows as dicts. Athena returns the header as the first row of the first page."""
    rows, header = [], None
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            vals = [c.get("VarCharValue") for c in r["Data"]]
            if header is None:
                header = vals
                continue
            rows.append(dict(zip(header, vals)))
    return rows


def run_and_fetch(sql: str, workgroup: str = "primary", database: str | None = None) -> list[dict]:
    return fetch(run(sql, workgroup, database))


def to_dataframe(sql: str, workgroup: str = "primary", database: str | None = None):
    import pandas as pd
    return pd.DataFrame(run_and_fetch(sql, workgroup, database))
```

### Example 3 — A query

`sql/athena/top_customers.sql`

```sql
SELECT customer_id, count(*) AS orders, round(sum(amount), 2) AS revenue
FROM orders_clean
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 10
```

### Example 4 — Run it

```python
from src.athena_ops import query, setup

wg = setup.bootstrap()
print(query.run_and_fetch(open("sql/athena/top_customers.sql").read(), wg, setup.DB))
# [{'customer_id': 'C-1', 'orders': '2', 'revenue': '15.5'}, ...]
```

### Example 5 — Real-AWS-only DDL (for reference)

`sql/athena/create_orders_ext.sql`

```sql
-- REAL AWS ONLY. On Floci register the table through the Glue API instead (src/glue_ops/catalog.py).
CREATE EXTERNAL TABLE IF NOT EXISTS floci_demo_db.orders_raw (
    order_id    string,
    customer_id string,
    amount      double,
    status      string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://floci-demo-raw/raw/orders/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

### Example 6 — Test

Writes a small curated CSV, bootstraps the workgroup and tables, runs the query, and asserts the revenue totals when a real SQL engine is present (Floci).

`tests/test_04_compute_and_catalog.py`

```python
import os
from src.athena_ops import query, setup as athena_setup
from src.s3_ops import buckets, notifications, objects

MOTO = os.getenv("EMULATOR") == "moto"  # moto cannot run Lambda containers or real SQL


def test_athena_queries():
    b = buckets.create_pipeline_buckets()
    objects.put_text(b["curated"], "clean/orders/orders.csv",
                     "order_id,customer_id,amount,status\nO-1,C-1,10.00,NEW\nO-2,C-1,5.50,NEW\nO-3,C-2,1.00,NEW\n")
    wg = athena_setup.bootstrap()
    sql = open("sql/athena/top_customers.sql").read()
    rows = query.run_and_fetch(sql, wg, athena_setup.DB)
    if not MOTO:  # real SQL engine (DuckDB in Floci)
        assert rows[0]["customer_id"] == "C-1"
        assert float(rows[0]["revenue"]) == 15.5
    assert isinstance(rows, list)
```

### Example 7 — Terraform

Tables are declared in `terraform/glue.tf` ([guide 03](03-glue.md)), because Athena reads the Glue catalog.

`terraform/athena.tf`

```hcl
resource "aws_athena_workgroup" "main" {
  name          = "${var.prefix}-wg"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["athena-results"].bucket}/results/"
    }
  }
}

resource "aws_athena_named_query" "top_customers" {
  name      = "top-customers"
  workgroup = aws_athena_workgroup.main.id
  database  = aws_glue_catalog_database.main.name
  query     = file("${path.module}/../sql/athena/top_customers.sql")
}
```

---

## Reference

Project files:

```
src/athena_ops/{setup,query}.py
sql/athena/top_customers.sql
terraform/athena.tf
```

Related: [03 Glue](03-glue.md) (tables), [01 S3](01-s3.md) (data and results), [15 End-to-end](15-end-to-end-pipeline.md).
