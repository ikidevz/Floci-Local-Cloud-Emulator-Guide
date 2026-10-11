# 14 — Project 4: Batch: Cloud SQL → Cloud Storage → BigQuery (nightly ETL)

> [Index](README.md) · [← 13 — Project 3: Telemetry Pipeline: Pub/Sub → Bigtable → BigQuery](13-project-telemetry.md) · [15 — Project 5: Stream: Pub/Sub → Windowed Aggregation → Firestore + BigQuery →](15-project-stream.md)

---

## 1. Documentation

### Overview

A **batch** job extracts yesterday's rows from Postgres (Cloud SQL), stages them as a file in Cloud Storage, loads them into a BigQuery staging table, then transforms into a reporting table with SQL. Wires **Cloud SQL + Cloud Storage + BigQuery**.

```
Postgres orders ─▶ extract (date window) ─▶ GCS staging/orders_<date>.jsonl ─▶ BQ load ─▶ SQL transform ─▶ daily_sales
```

### At a glance

| Item | Value |
|------|-------|
| Wires | Cloud SQL + Cloud Storage + BigQuery |
| Compose services | `postgres gcs bigquery` |
| Script | `p4_batch.py` (code is in Example 1; save it in `project/`) |
| Environment | `source .env.local` |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d postgres gcs bigquery
source .env.local
python p4_batch.py 2026-10-10        # run for one date; re-running is safe (idempotent)
```

---

## 3. Examples

### Example 1 — `p4_batch.py`

```python
import json, sys, datetime as dt
import psycopg2
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, storage

P = "demo-project"
run_date = sys.argv[1] if len(sys.argv) > 1 else str(dt.date.today() - dt.timedelta(days=1))

pg = psycopg2.connect(host="localhost", dbname="app", user="postgres", password="postgres")
gcs = storage.Client(project=P, credentials=AnonymousCredentials())
bq = bigquery.Client(project=P, credentials=AnonymousCredentials(),
                     client_options=ClientOptions(api_endpoint="http://localhost:9050"))

# --- seed source data (demo only) ---
with pg, pg.cursor() as c:
    c.execute("""CREATE TABLE IF NOT EXISTS orders(
        id SERIAL PRIMARY KEY, customer TEXT, amount NUMERIC(10,2), created_at DATE)""")
    c.execute("SELECT COUNT(*) FROM orders")
    if c.fetchone()[0] == 0:
        c.executemany("INSERT INTO orders(customer, amount, created_at) VALUES (%s,%s,%s)", [
            ("ana", 10.50, "2026-10-10"), ("ben", 20.00, "2026-10-10"),
            ("ana", 5.25, "2026-10-10"), ("cara", 99.99, "2026-10-09")])

# 1) EXTRACT one date window
with pg, pg.cursor() as c:
    c.execute("SELECT id, customer, amount::float, created_at::text FROM orders WHERE created_at = %s", (run_date,))
    rows = [dict(zip(("id", "customer", "amount", "created_at"), r)) for r in c.fetchall()]
print(f"extracted {len(rows)} rows for {run_date}")

# 2) STAGE in GCS (newline-delimited JSON)
bucket = gcs.bucket("staging")
if not bucket.exists():
    gcs.create_bucket("staging")
path = f"orders/dt={run_date}/orders.jsonl"
bucket.blob(path).upload_from_string("\n".join(json.dumps(r) for r in rows))

# 3) LOAD into BigQuery staging (replace that day's data -> idempotent)
schema = [bigquery.SchemaField("id", "INTEGER"), bigquery.SchemaField("customer", "STRING"),
          bigquery.SchemaField("amount", "FLOAT"), bigquery.SchemaField("created_at", "DATE")]
bq.create_table(bigquery.Table(f"{P}.demo.stg_orders", schema=schema), exists_ok=True)
bq.query(f"DELETE FROM `{P}.demo.stg_orders` WHERE created_at = '{run_date}'").result()
data = bucket.blob(path).download_as_text().splitlines()
errs = bq.insert_rows_json(f"{P}.demo.stg_orders", [json.loads(x) for x in data])
assert not errs, errs
# On real BigQuery use a load job straight from GCS instead:
#   bq.load_table_from_uri(f"gs://staging/{path}", f"{P}.demo.stg_orders",
#       job_config=bigquery.LoadJobConfig(source_format="NEWLINE_DELIMITED_JSON", schema=schema)).result()

# 4) TRANSFORM -> reporting table
bq.create_table(bigquery.Table(f"{P}.demo.daily_sales", schema=[
    bigquery.SchemaField("day", "DATE"), bigquery.SchemaField("customer", "STRING"),
    bigquery.SchemaField("total", "FLOAT"), bigquery.SchemaField("orders", "INTEGER")]), exists_ok=True)
bq.query(f"DELETE FROM `{P}.demo.daily_sales` WHERE day = '{run_date}'").result()
bq.query(f"""INSERT INTO `{P}.demo.daily_sales`
             SELECT created_at, customer, SUM(amount), COUNT(*)
             FROM `{P}.demo.stg_orders` WHERE created_at = '{run_date}' GROUP BY created_at, customer""").result()

for r in bq.query(f"SELECT * FROM `{P}.demo.daily_sales` ORDER BY customer").result():
    print(dict(r))
```

---

## Reference

### Extend it

- Schedule nightly: Cloud Scheduler → Cloud Run job (guide 10) running `p4_batch.py`.
- Swap the insert for a real load job from `gs://` when you move to real BigQuery.
- Add a row-count check between Postgres and BigQuery and fail the run if they differ.

Guides for the services this project wires: see [README](README.md#guide-index).
