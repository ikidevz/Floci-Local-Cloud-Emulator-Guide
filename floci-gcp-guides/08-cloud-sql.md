# 08 — Cloud SQL (PostgreSQL)

> [Index](README.md) · [← 07 — Cloud Tasks](07-cloud-tasks.md) · [09 — Cloud Functions →](09-cloud-functions.md)

---

## 1. Documentation

### Overview

Cloud SQL is managed MySQL/PostgreSQL/SQL Server. Google runs backups, patching, and failover; you just connect with a normal SQL driver. It's the RDS of GCP.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | plain Postgres 16 |
| AWS equivalent | RDS |
| Compose service | `postgres` |
| Host port(s) | `5432` |
| Environment variable | none (host `localhost`, db `app`, user/password `postgres`) |
| Python package | `psycopg2-binary` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

There's no Cloud SQL emulator, but the engine *is* Postgres, so run Postgres locally and use the same code:
```bash
docker compose up -d postgres
```
In production you connect via the Cloud SQL Auth Proxy or the `cloud-sql-python-connector`; only the host/connection setup changes.

---

## 3. Examples

### Example 1 — Core flow

```python
import psycopg2

conn = psycopg2.connect(host="localhost", dbname="app", user="postgres", password="postgres")
cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS customers(
    id SERIAL PRIMARY KEY, name TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now())""")
cur.executemany("INSERT INTO customers(name) VALUES (%s)", [("Ana",), ("Ben",)])
conn.commit()

cur.execute("SELECT id, name FROM customers ORDER BY id")
print(cur.fetchall())
conn.close()
```
Production connection (real Cloud SQL):
```python
from google.cloud.sql.connector import Connector
conn = Connector().connect("PROJECT:REGION:INSTANCE", "pg8000", user="app", password="...", db="app")
```
### Example 2 — Terraform (real GCP)

```hcl
resource "google_sql_database_instance" "pg" {
  name             = "demo-pg"
  database_version = "POSTGRES_16"
  region           = "us-central1"
  settings { tier = "db-f1-micro" }
  deletion_protection = false
}
resource "google_sql_database" "app" { name = "app" instance = google_sql_database_instance.pg.name }
```

---

## Reference

Project files (shared by every GCP guide):

```
docker-compose.yml   (service: postgres)
.env.local
requirements.txt
```

Related: [09 RDS Postgres](../floci-aws-guides/09-rds-postgres.md). Projects that use this service are listed in the [README](README.md#projects).
