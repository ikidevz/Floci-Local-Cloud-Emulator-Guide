# 09 — RDS (Postgres)

> [Index](README.md) · [← 08 — CloudWatch](08-cloudwatch.md) · [10 — Athena →](10-athena.md)

A real PostgreSQL instance per `create_db_instance`: schema, load from S3, and query.

---

## 1. Documentation

### Overview

- Floci starts a **real PostgreSQL container** per RDS instance and proxies TCP to it.
- You manage the instance with the RDS API, then connect with a normal Postgres driver (`psycopg2`).
- In this project curated orders from S3 are upserted into an `orders` table.

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `rds` |
| Terraform endpoint | `rds` |
| Needs first | curated data in S3 ([01](01-s3.md)) if you want to load it |
| Docker socket | **required** |
| Ports | publish `7001-7099` (done in [00](00-setup-floci.md)) |

### How it behaves on Floci

- Publish the proxy range `7001-7099` in compose (done in 00).
- Connect to the host/port in `describe_db_instances`, **not** 5432.
- Creation takes a while: wait for `available`.
- From another container (such as a Lambda), `localhost` is that container; use the Docker-network address instead.

### Key concepts

- **Endpoint**: `describe_db_instances` returns `Endpoint.Address` and `Endpoint.Port`. The port is a proxy port.
- **`PG_HOST` / `PG_PORT`**: environment overrides for the endpoint, handy for a local Postgres or inside Docker networks.
- **Upsert**: `INSERT … ON CONFLICT (order_id) DO UPDATE` keeps loads re-runnable without duplicates.

### Gotchas

- Pick an engine version Floci supports; Floci's own Terraform suite uses a major-only version (`"15"`), as this guide does with `"16"`.
- Keep passwords out of Git; on real AWS use `manage_master_user_password` or Secrets Manager.

---

## 2. Basic Usage

> Prerequisite: Floci is running with the Docker socket mounted and ports `7001-7099` published, and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

### Create the instance and find its endpoint

```python
import boto3

rds = boto3.client("rds")

rds.create_db_instance(
    DBInstanceIdentifier="demo-pg", Engine="postgres", EngineVersion="16",
    DBInstanceClass="db.t3.micro", AllocatedStorage=20,
    DBName="appdb", MasterUsername="app", MasterUserPassword="ChangeMe123!",
    PubliclyAccessible=True, BackupRetentionPeriod=0)

rds.get_waiter("db_instance_available").wait(
    DBInstanceIdentifier="demo-pg", WaiterConfig={"Delay": 3, "MaxAttempts": 100})

endpoint = rds.describe_db_instances(DBInstanceIdentifier="demo-pg")["DBInstances"][0]["Endpoint"]
print(endpoint["Address"], endpoint["Port"])    # a proxy port in 7001-7099, not 5432
```

### Connect and query

```python
# (needs Floci's Postgres container, or any Postgres reachable at the endpoint)
import psycopg2

conn = psycopg2.connect(host=endpoint["Address"], port=int(endpoint["Port"]), dbname="appdb",
                        user="app", password="ChangeMe123!", connect_timeout=10)
with conn, conn.cursor() as cur:          # `with conn` commits on success
    cur.execute("CREATE TABLE IF NOT EXISTS hello (id int PRIMARY KEY, msg text)")
    cur.execute("INSERT INTO hello VALUES (1, 'hi') ON CONFLICT DO NOTHING")
    cur.execute("SELECT msg FROM hello WHERE id = 1")
    print(cur.fetchone())                 # ('hi',)
conn.close()
```

---

## 3. Examples

### Example 1 — Instance helpers (create and wait, describe, endpoint, snapshot, delete)

`src/rds_ops/instances.py`

```python
from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

rds = client("rds")


def create_postgres(identifier: str, db_name: str = "appdb", user: str = "app",
                    password: str = "ChangeMe123!", version: str = "16",
                    instance_class: str = "db.t3.micro", storage_gb: int = 20) -> dict:
    """Create a Postgres instance (Floci starts a real container) and wait for 'available'."""
    try:
        rds.create_db_instance(
            DBInstanceIdentifier=identifier, Engine="postgres", EngineVersion=version,
            DBInstanceClass=instance_class, AllocatedStorage=storage_gb,
            DBName=db_name, MasterUsername=user, MasterUserPassword=password,
            PubliclyAccessible=True, BackupRetentionPeriod=0)
    except ClientError as e:
        if e.response["Error"]["Code"] != "DBInstanceAlreadyExists":
            raise
    wait_until(lambda: describe(identifier)["DBInstanceStatus"],
               done={"available"}, failed={"failed", "incompatible-parameters"}, timeout=300, interval=3)
    return describe(identifier)


def describe(identifier: str) -> dict:
    return rds.describe_db_instances(DBInstanceIdentifier=identifier)["DBInstances"][0]


def endpoint(identifier: str) -> tuple[str, int]:
    ep = describe(identifier)["Endpoint"]
    return ep["Address"], int(ep["Port"])


def snapshot(identifier: str, snapshot_id: str) -> None:
    rds.create_db_snapshot(DBInstanceIdentifier=identifier, DBSnapshotIdentifier=snapshot_id)


def delete(identifier: str) -> None:
    rds.delete_db_instance(DBInstanceIdentifier=identifier, SkipFinalSnapshot=True,
                           DeleteAutomatedBackups=True)
```

### Example 2 — Connection helper with `PG_HOST` override

`src/rds_ops/db.py`

```python
import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from src.rds_ops.instances import endpoint


def _target(identifier: str) -> tuple[str, int]:
    """PG_HOST/PG_PORT override the RDS endpoint (handy for a local Postgres or inside containers)."""
    if os.getenv("PG_HOST"):
        return os.environ["PG_HOST"], int(os.getenv("PG_PORT", "5432"))
    return endpoint(identifier)  # use what Floci returns; the port is a proxy port, not 5432


@contextmanager
def connect(identifier: str, db_name: str = "appdb", user: str = "app", password: str = "ChangeMe123!"):
    host, port = _target(identifier)
    conn = psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password,
                            connect_timeout=10)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query(conn, sql: str, params: tuple | None = None) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()] if cur.description else []
```

### Example 3 — Schema

`sql/schema.sql`

```sql
CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount      NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    status      TEXT NOT NULL DEFAULT 'NEW',
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
```

### Example 4 — Apply the schema and load curated orders from S3

`src/rds_ops/load.py`

```python
import csv
import io
from pathlib import Path

import psycopg2.extras

from src.common.aws import client
from src.rds_ops.db import connect


def apply_schema(identifier: str, path: str = "sql/schema.sql") -> None:
    with connect(identifier) as conn, conn.cursor() as cur:
        cur.execute(Path(path).read_text())


def load_csv_from_s3(identifier: str, bucket: str, key: str) -> int:
    """Upsert curated orders into Postgres. Returns rows written."""
    body = client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    rows = [(r["order_id"], r["customer_id"], r["amount"], r.get("status") or "NEW")
            for r in csv.DictReader(io.StringIO(body))]
    with connect(identifier) as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO orders (order_id, customer_id, amount, status) VALUES %s
               ON CONFLICT (order_id) DO UPDATE
               SET amount = EXCLUDED.amount, status = EXCLUDED.status""",
            rows)
    return len(rows)
```

### Example 5 — End to end

```python
from src.common.aws import name
from src.rds_ops import instances, load
from src.rds_ops.db import connect, query

ident = name("pg")
print(instances.create_postgres(ident)["Endpoint"])
load.apply_schema(ident)
print(load.load_csv_from_s3(ident, name("curated"), "clean/orders/orders.csv"))
with connect(ident) as c:
    print(query(c, "SELECT count(*) AS n FROM orders"))
```

### Example 6 — Test

Verified against a real local Postgres by setting `PG_HOST` (the schema, upsert and query code ran for real). On Floci it creates the instance itself.

`tests/test_05_rds.py`

```python
import os

import pytest

from src.common.aws import name
from src.rds_ops import db, instances, load
from src.s3_ops import buckets, objects

MOTO = os.getenv("EMULATOR") == "moto"
LOCAL_PG = bool(os.getenv("PG_HOST"))


@pytest.mark.skipif(MOTO and not LOCAL_PG, reason="needs real Postgres (Floci RDS, or PG_HOST)")
def test_rds_schema_load_and_query():
    ident = name("pg")
    if not LOCAL_PG:
        info = instances.create_postgres(ident)
        assert info["Endpoint"]["Port"]
    b = buckets.create_pipeline_buckets()
    objects.put_text(b["curated"], "clean/orders/orders.csv",
                     "order_id,customer_id,amount,status\nO-1,C-1,10.00,NEW\nO-2,C-1,5.50,NEW\n")
    load.apply_schema(ident)
    assert load.load_csv_from_s3(ident, b["curated"], "clean/orders/orders.csv") == 2
    assert load.load_csv_from_s3(ident, b["curated"], "clean/orders/orders.csv") == 2  # upsert, no dupes
    with db.connect(ident) as conn:
        rows = db.query(conn, "SELECT customer_id, sum(amount) AS total FROM orders GROUP BY 1")
    assert rows[0]["customer_id"] == "C-1" and float(rows[0]["total"]) == 15.5
```

```bash
PG_HOST=127.0.0.1 PG_PORT=5432 pytest tests/test_05_rds.py   # local Postgres
pytest tests/test_05_rds.py                                  # Floci RDS
```

### Example 7 — Terraform

`terraform/rds.tf`

```hcl
resource "aws_db_instance" "postgres" {
  identifier              = "${var.prefix}-pg"
  engine                  = "postgres"
  engine_version          = "16"
  instance_class          = "db.t3.micro"
  allocated_storage       = 20
  db_name                 = "appdb"
  username                = "app"
  password                = var.db_password
  publicly_accessible     = true
  skip_final_snapshot     = true
  backup_retention_period = 0
  tags                    = local.tags
}
```

---

## Reference

Project files:

```
src/rds_ops/{instances,db,load}.py
sql/schema.sql
terraform/rds.tf
tests/test_05_rds.py
```

Related: [01 S3](01-s3.md) (curated data), [03 Glue](03-glue.md) (produces it), [00 Setup](00-setup-floci.md) (ports `7001-7099`).
