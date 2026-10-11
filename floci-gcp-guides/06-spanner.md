# 06 — Spanner

> [Index](README.md) · [← 05 — Bigtable](05-bigtable.md) · [07 — Cloud Tasks →](07-cloud-tasks.md)

---

## 1. Documentation

### Overview

Spanner is a globally distributed relational database with SQL and strong consistency. Use it when you need relational structure at very large scale.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | Spanner emulator |
| AWS equivalent | Aurora |
| Compose service | `spanner` |
| Host port(s) | `9010`, `9020` |
| Environment variable | `SPANNER_EMULATOR_HOST` |
| Python package | `google-cloud-spanner` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d spanner
source .env.local            # sets SPANNER_EMULATOR_HOST
```
Hierarchy: **instance** → **database** → tables.

---

## 3. Examples

### Example 1 — Core flow

```python
from google.cloud import spanner

client = spanner.Client(project="demo-project")
instance = client.instance("demo-instance", configuration_name="projects/demo-project/instanceConfigs/emulator-config", node_count=1)
if not instance.exists():
    instance.create().result()

db = instance.database("appdb", ddl_statements=[
    """CREATE TABLE Singers (
         SingerId INT64 NOT NULL,
         Name STRING(100)
       ) PRIMARY KEY (SingerId)"""
])
if not db.exists():
    db.create().result()

# write
with db.batch() as b:
    b.insert("Singers", columns=("SingerId", "Name"), values=[(1, "Ana"), (2, "Ben")])

# read via SQL
with db.snapshot() as s:
    for row in s.execute_sql("SELECT SingerId, Name FROM Singers ORDER BY SingerId"):
        print(row)

# transaction
def rename(tx):
    tx.execute_update("UPDATE Singers SET Name = 'Anna' WHERE SingerId = 1")
db.run_in_transaction(rename)
```
### Example 2 — Terraform (real GCP)

```hcl
resource "google_spanner_instance" "i" {
  name = "demo-instance" config = "regional-us-central1"
  display_name = "demo" num_nodes = 1
}
resource "google_spanner_database" "d" {
  instance = google_spanner_instance.i.name
  name     = "appdb"
  ddl      = ["CREATE TABLE Singers (SingerId INT64 NOT NULL, Name STRING(100)) PRIMARY KEY (SingerId)"]
}
```

---

## Reference

Project files (shared by every GCP guide):

```
docker-compose.yml   (service: spanner)
.env.local
requirements.txt
```

Related: Projects that use this service are listed in the [README](README.md#projects).
