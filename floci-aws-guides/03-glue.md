# 03 — Glue (Catalog, Jobs, Crawlers)

> [Index](README.md) · [← 02 — Lambda](02-lambda.md) · [04 — Step Functions →](04-step-functions.md)

Data Catalog tables that Athena reads, plus job/crawler lifecycle and a local ETL runner.

---

## 1. Documentation

### Overview

- **Data Catalog**: databases and tables (and partitions) describing data that lives in S3. Athena queries these tables.
- **Jobs**: ETL scripts (`pythonshell` or `glueetl`/Spark) started with `StartJobRun`.
- **Crawlers**: scan S3 and create tables (on real AWS).

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `glue` |
| Terraform endpoint | `glue` |
| Needs first | an IAM role ([07](07-iam.md)), the script bucket ([01](01-s3.md)) |
| Docker socket | not required |

### What Floci actually does (read this first)

| Feature | On Floci | On real AWS |
|---------|----------|-------------|
| Data Catalog (databases, tables, partitions) | **fully emulated** | yes |
| Jobs (`StartJobRun`) | follows Glue's state machine (`RUNNING` → `SUCCEEDED`, `STOPPED`, `TIMEOUT`) but **does not execute your script** | runs the script |
| Crawlers (`StartCrawler`) | goes `RUNNING` → `READY` with `LastCrawl = SUCCEEDED`, but **does not read S3 or create tables** | discovers schemas and writes tables |
| Crawler schedules | stored, never fire on their own | fire |

So this guide does three things:

1. Registers tables **explicitly** through the catalog API.
2. Exercises the job/crawler lifecycle and polling exactly as on real AWS.
3. Provides `etl_local.py`, which **runs the same transformation code locally** against Floci to produce the curated data.

### Key concepts

- **`pythonshell`** jobs run a single Python file (self-contained, so it is also run locally). **`glueetl`** jobs run Spark and need the Glue runtime, so they only run on real AWS.
- Job arguments are passed as `--KEY value` pairs.

### Gotchas

- `ensure_crawler` skips the update when nothing changed. Updating a crawler that is `RUNNING` is refused (`CrawlerRunningException`).
- Table `Location` must be a folder prefix ending in `/`.
- `glue_scripts/clean_orders_spark.py` needs the Glue runtime (`awsglue`, `pyspark`), so it only runs on real AWS.
- Catalog tables are what Athena queries (see [10](10-athena.md)).

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

### Register a database and a table

```python
import json
import boto3

glue, iam = boto3.client("glue"), boto3.client("iam")

glue.create_database(DatabaseInput={"Name": "demo_db"})
glue.create_table(
    DatabaseName="demo_db",
    TableInput={
        "Name": "orders_raw",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
        "StorageDescriptor": {
            "Columns": [{"Name": "order_id", "Type": "string"}, {"Name": "amount", "Type": "double"}],
            "Location": "s3://demo-raw/raw/orders/",  # folder prefix, ends with '/'
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                          "Parameters": {"field.delim": ","}},
        },
    },
)
print([t["Name"] for t in glue.get_tables(DatabaseName="demo_db")["TableList"]])   # ['orders_raw']
```

### Start a job and read its state

```python
trust = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
role_arn = iam.create_role(RoleName="demo-glue-role",
                           AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]

glue.create_job(Name="demo-clean", Role=role_arn, MaxCapacity=0.0625,
                Command={"Name": "pythonshell", "PythonVersion": "3.9",
                         "ScriptLocation": "s3://demo-scripts/glue/clean.py"})
run_id = glue.start_job_run(JobName="demo-clean")["JobRunId"]
print(glue.get_job_run(JobName="demo-clean", RunId=run_id)["JobRun"]["JobRunState"])  # RUNNING, then SUCCEEDED
```

> On Floci the state changes but the script is **not executed**. Use `etl_local` (Example 6) to produce the output.

---

## 3. Examples

### Example 1 — Catalog helpers (database, CSV table, list, columns)

`src/glue_ops/catalog.py`

```python
"""
Glue Data Catalog. On Floci, crawlers do NOT discover schemas or write tables,
so register tables explicitly with create_csv_table (Athena reads these tables).
"""
from botocore.exceptions import ClientError

from src.common.aws import client

glue = client("glue")

ORDERS_COLUMNS = {"order_id": "string", "customer_id": "string", "amount": "double", "status": "string"}


def ensure_database(db: str, description: str = "") -> None:
    try:
        glue.create_database(DatabaseInput={"Name": db, "Description": description})
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise


def ensure_csv_table(db: str, table: str, location: str, columns: dict[str, str]) -> None:
    """location must be a folder prefix ending in '/'."""
    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
        "StorageDescriptor": {
            "Columns": [{"Name": k, "Type": v} for k, v in columns.items()],
            "Location": location,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                          "Parameters": {"field.delim": ","}},
        },
    }
    try:
        glue.create_table(DatabaseName=db, TableInput=table_input)
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        glue.update_table(DatabaseName=db, TableInput=table_input)


def list_tables(db: str) -> list[str]:
    return [t["Name"] for p in glue.get_paginator("get_tables").paginate(DatabaseName=db)
            for t in p["TableList"]]


def get_columns(db: str, table: str) -> dict[str, str]:
    cols = glue.get_table(DatabaseName=db, Name=table)["Table"]["StorageDescriptor"]["Columns"]
    return {c["Name"]: c["Type"] for c in cols}
```

### Example 2 — Crawler: create, update, run and wait

`src/glue_ops/crawlers.py`

```python
from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

glue = client("glue")


def ensure_crawler(crawler: str, role_arn: str, db: str, s3_path: str, schedule: str | None = None) -> None:
    kwargs = dict(Name=crawler, Role=role_arn, DatabaseName=db,
                  Targets={"S3Targets": [{"Path": s3_path}]},
                  SchemaChangePolicy={"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"})
    if schedule:
        kwargs["Schedule"] = schedule  # e.g. "cron(0 2 * * ? *)"
    try:
        glue.create_crawler(**kwargs)
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        cur = glue.get_crawler(Name=crawler)["Crawler"]
        same = (cur["Role"].endswith(role_arn.split("/")[-1]) and cur["DatabaseName"] == db
                and cur["Targets"].get("S3Targets", [{}])[0].get("Path") == s3_path)
        if same:
            return  # nothing to change (also avoids CrawlerRunningException on a busy crawler)
        glue.update_crawler(**kwargs)


def run_crawler(crawler: str, timeout: int = 300) -> str:
    """Start a crawl and wait for READY. Returns LastCrawl.Status (SUCCEEDED/FAILED/CANCELLED)."""
    glue.start_crawler(Name=crawler)
    wait_until(lambda: glue.get_crawler(Name=crawler)["Crawler"]["State"],
               done={"READY"}, timeout=timeout, interval=1)
    return glue.get_crawler(Name=crawler)["Crawler"].get("LastCrawl", {}).get("Status", "UNKNOWN")
```

### Example 3 — Jobs: upload script, create/update, run and wait

`src/glue_ops/jobs.py`

```python
from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

glue = client("glue")
s3 = client("s3")

TERMINAL_FAIL = {"FAILED", "ERROR", "TIMEOUT", "STOPPED"}


def upload_script(bucket: str, local_path: str, key: str) -> str:
    s3.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


def ensure_job(job: str, role_arn: str, script_s3: str, shell: bool = True,
               default_args: dict | None = None) -> None:
    kwargs = dict(
        Name=job, Role=role_arn,
        Command={"Name": "pythonshell" if shell else "glueetl", "ScriptLocation": script_s3,
                 "PythonVersion": "3.9" if shell else "3"},
        DefaultArguments={"--job-language": "python", **(default_args or {})},
        MaxRetries=0, Timeout=30,
    )
    if shell:
        kwargs["MaxCapacity"] = 0.0625
    else:
        kwargs.update(GlueVersion="4.0", WorkerType="G.1X", NumberOfWorkers=2)
    try:
        glue.create_job(**kwargs)
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        glue.update_job(JobName=job, JobUpdate={k: v for k, v in kwargs.items() if k != "Name"})


def start(job: str, arguments: dict | None = None) -> str:
    return glue.start_job_run(JobName=job, Arguments=arguments or {})["JobRunId"]


def run_and_wait(job: str, arguments: dict | None = None, timeout: int = 900) -> str:
    """Start a run and poll until SUCCEEDED. Raises on FAILED/ERROR/TIMEOUT/STOPPED."""
    run_id = start(job, arguments)
    return wait_until(lambda: glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]["JobRunState"],
                      done={"SUCCEEDED"}, failed=TERMINAL_FAIL, timeout=timeout, interval=1)
```

### Example 4 — Python-shell job script

De-duplicates orders and drops non-positive amounts. Self-contained on purpose: Glue runs this single file, and the same `clean()` is reused by the local runner.

`glue_scripts/clean_orders_shell.py`

```python
"""
Glue *Python shell* job: de-duplicate orders and drop non-positive amounts.

Glue passes arguments as:  --SOURCE_BUCKET b --SOURCE_KEY k --TARGET_BUCKET b --TARGET_KEY k
Self-contained on purpose: Glue runs this single file. The same clean() is reused by the local
runner (src/glue_ops/etl_local.py), because Floci's StartJobRun does NOT execute scripts.
"""
import csv
import io
import sys


def clean(rows: list[dict]) -> list[dict]:
    seen, out = set(), []
    for r in rows:
        try:
            amount = float(r["amount"])
        except (KeyError, ValueError):
            continue
        if amount <= 0 or r["order_id"] in seen:
            continue
        seen.add(r["order_id"])
        out.append({**r, "amount": f"{amount:.2f}"})
    return out


def to_csv(rows: list[dict], fieldnames: list[str]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def parse_args(argv: list[str]) -> dict:
    return {k.lstrip("-"): v for k, v in zip(argv[0::2], argv[1::2]) if k.startswith("--")}


def main(argv: list[str]) -> None:
    import boto3  # imported here so unit tests of clean() need no AWS
    a = parse_args(argv)
    s3 = boto3.client("s3")
    body = s3.get_object(Bucket=a["SOURCE_BUCKET"], Key=a["SOURCE_KEY"])["Body"].read().decode()
    reader = csv.DictReader(io.StringIO(body))
    rows = clean(list(reader))
    s3.put_object(Bucket=a["TARGET_BUCKET"], Key=a["TARGET_KEY"],
                  Body=to_csv(rows, reader.fieldnames).encode())
    print(f"kept {len(rows)} rows")


if __name__ == "__main__":
    main(sys.argv[1:])
```

### Example 5 — Spark job (real AWS only)

`glue_scripts/clean_orders_spark.py`

```python
"""Glue Spark (glueetl) variant for real AWS. Not executed by Floci."""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

df = spark.read.option("header", True).csv(args["SOURCE_PATH"])
clean = (df.dropDuplicates(["order_id"])
           .withColumn("amount", F.col("amount").cast("double"))
           .filter(F.col("amount") > 0))
clean.write.mode("overwrite").option("header", True).csv(args["TARGET_PATH"])
job.commit()
```

### Example 6 — Run the job logic locally against Floci

`src/glue_ops/etl_local.py`

```python
"""
Run the Glue job's logic locally against the emulator.

Floci's StartJobRun follows Glue's state machine but does not execute the script, so
use this to actually produce the curated data (it calls the same clean() the Glue job uses).
"""
from glue_scripts.clean_orders_shell import main

from src.common.aws import name


def run_clean_orders(source_key: str = "raw/orders/orders.csv",
                     target_key: str = "clean/orders/orders.csv") -> None:
    main(["--SOURCE_BUCKET", name("raw"), "--SOURCE_KEY", source_key,
          "--TARGET_BUCKET", name("curated"), "--TARGET_KEY", target_key])
```

```python
from data.make_orders import make
from src.common.aws import name
from src.s3_ops import buckets, objects
from src.glue_ops import etl_local

b = buckets.create_pipeline_buckets()
objects.upload_file(b["raw"], "raw/orders/orders.csv", make())
etl_local.run_clean_orders()      # writes s3://<prefix>-curated/clean/orders/orders.csv
```

### Example 7 — Tests

`tests/test_04_compute_and_catalog.py`

```python
import os
from data.make_orders import make
from src.athena_ops import query, setup as athena_setup
from src.common.aws import arn, client, name
from src.glue_ops import catalog, crawlers, etl_local, jobs
from src.iam_ops import roles
from src.s3_ops import buckets, notifications, objects

MOTO = os.getenv("EMULATOR") == "moto"  # moto cannot run Lambda containers or real SQL


def test_clean_logic():
    from glue_scripts.clean_orders_shell import clean, parse_args
    rows = [{"order_id": "1", "amount": "10"}, {"order_id": "1", "amount": "10"},
            {"order_id": "2", "amount": "-1"}, {"order_id": "3", "amount": "abc"},
            {"order_id": "4", "amount": "2.5"}]
    assert [r["order_id"] for r in clean(rows)] == ["1", "4"]
    assert parse_args(["--A", "1", "--B", "2"]) == {"A": "1", "B": "2"}


def test_glue_catalog_job_crawler_and_local_etl():
    b = buckets.create_pipeline_buckets()
    role = roles.create_all()["glue-role"]
    objects.upload_file(b["raw"], "raw/orders/orders.csv", make("/tmp/orders_t.csv"))

    catalog.ensure_database(athena_setup.DB)
    catalog.ensure_csv_table(athena_setup.DB, "orders_raw", f"s3://{b['raw']}/raw/orders/",
                             catalog.ORDERS_COLUMNS)
    catalog.ensure_csv_table(athena_setup.DB, "orders_raw", f"s3://{b['raw']}/raw/orders/",
                             catalog.ORDERS_COLUMNS)  # idempotent (update path)
    assert "orders_raw" in catalog.list_tables(athena_setup.DB)
    assert catalog.get_columns(athena_setup.DB, "orders_raw")["amount"] == "double"

    crawlers.ensure_crawler(name("crawler"), role, athena_setup.DB, f"s3://{b['raw']}/raw/orders/")
    if MOTO:  # moto leaves crawlers RUNNING forever; Floci goes RUNNING -> READY (see Glue docs)
        client("glue").start_crawler(Name=name("crawler"))
    else:
        assert crawlers.run_crawler(name("crawler"), timeout=60) == "SUCCEEDED"

    script = jobs.upload_script(b["scripts"], "glue_scripts/clean_orders_shell.py", "glue/clean.py")
    jobs.ensure_job(name("clean-orders"), role, script, shell=True)
    jobs.ensure_job(name("clean-orders"), role, script, shell=True)  # idempotent (update path)
    assert jobs.run_and_wait(name("clean-orders"), {"--X": "1"}, timeout=60) == "SUCCEEDED"

    etl_local.run_clean_orders()
    text = objects.get_text(b["curated"], "clean/orders/orders.csv")
    lines = text.strip().splitlines()
    assert lines[0] == "order_id,customer_id,amount,status"
    assert len(lines) == 1 + 50  # 52 raw rows - 1 duplicate - 1 negative amount
```

### Example 8 — Terraform

`terraform/glue.tf`

```hcl
resource "aws_glue_catalog_database" "main" {
  name = "floci_demo_db"
}

# Floci crawlers do not write tables, so declare them explicitly.
resource "aws_glue_catalog_table" "orders_raw" {
  name          = "orders_raw"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification            = "csv"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.this["raw"].bucket}/raw/orders/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      parameters            = { "field.delim" = "," }
    }

    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "customer_id"
      type = "string"
    }
    columns {
      name = "amount"
      type = "double"
    }
    columns {
      name = "status"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "orders_clean" {
  name          = "orders_clean"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification            = "csv"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.this["curated"].bucket}/clean/orders/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      parameters            = { "field.delim" = "," }
    }

    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "customer_id"
      type = "string"
    }
    columns {
      name = "amount"
      type = "double"
    }
    columns {
      name = "status"
      type = "string"
    }
  }
}

resource "aws_s3_object" "clean_orders_script" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "glue/clean_orders_shell.py"
  source = "${path.module}/../glue_scripts/clean_orders_shell.py"
  etag   = filemd5("${path.module}/../glue_scripts/clean_orders_shell.py")
}

resource "aws_glue_crawler" "orders" {
  name          = "${var.prefix}-orders-crawler"
  role          = aws_iam_role.this["glue"].arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.this["raw"].bucket}/raw/orders/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_job" "clean_orders" {
  name         = "${var.prefix}-clean-orders"
  role_arn     = aws_iam_role.this["glue"].arn
  max_capacity = 0.0625
  max_retries  = 0
  timeout      = 30

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.this["scripts"].bucket}/${aws_s3_object.clean_orders_script.key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language"   = "python"
    "--SOURCE_BUCKET"  = aws_s3_bucket.this["raw"].bucket
    "--SOURCE_KEY"     = "raw/orders/orders.csv"
    "--TARGET_BUCKET"  = aws_s3_bucket.this["curated"].bucket
    "--TARGET_KEY"     = "clean/orders/orders.csv"
  }
}
```

---

## Reference

Project files:

```
glue_scripts/clean_orders_shell.py   # Python-shell job (self-contained, also run locally)
glue_scripts/clean_orders_spark.py   # Spark job for real AWS
src/glue_ops/{catalog,crawlers,jobs,etl_local}.py
terraform/glue.tf
```

Related: [01 S3](01-s3.md) (data and scripts), [10 Athena](10-athena.md) (queries these tables), [04 Step Functions](04-step-functions.md) (drives jobs through the `glue-runner` Lambda).
