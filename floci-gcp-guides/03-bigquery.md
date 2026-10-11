# 03 — BigQuery

> [Index](README.md) · [← 02 — Pub/Sub](02-pubsub.md) · [04 — Firestore →](04-firestore.md)

---

## 1. Documentation

### Overview

BigQuery is Google's serverless data warehouse. You load data into **tables** inside **datasets** and query it with standard SQL. Think Athena + Redshift in one.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | goccy/bigquery-emulator |
| AWS equivalent | Athena / Redshift |
| Compose service | `bigquery` |
| Host port(s) | `9050`, `9060` |
| Environment variable | `BIGQUERY_EMULATOR_HOST` |
| Python package | `google-cloud-bigquery` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d bigquery
```
The emulator speaks HTTP on `9050`. Point the client at it with `api_endpoint`.

---

## 3. Examples

### Example 1 — Core flow

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

client = bigquery.Client(
    project="demo-project",
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:9050"),
)

# table
schema = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("amount", "FLOAT"),
]
client.create_table(bigquery.Table("demo-project.demo.sales", schema=schema), exists_ok=True)

# insert rows
errors = client.insert_rows_json("demo-project.demo.sales", [
    {"id": 1, "name": "ana", "amount": 10.5},
    {"id": 2, "name": "ben", "amount": 20.0},
])
print("errors:", errors)

# query
for row in client.query("SELECT name, SUM(amount) AS total FROM `demo-project.demo.sales` GROUP BY name").result():
    print(row.name, row.total)
```
### Example 2 — Terraform (real GCP)

```hcl
resource "google_bigquery_dataset" "demo" { dataset_id = "demo" location = "US" }
resource "google_bigquery_table" "sales" {
  dataset_id = google_bigquery_dataset.demo.dataset_id
  table_id   = "sales"
  schema = jsonencode([
    { name = "id",     type = "INTEGER" },
    { name = "name",   type = "STRING"  },
    { name = "amount", type = "FLOAT"   },
  ])
}
```

---

## Reference

Project files (shared by every GCP guide):

```
docker-compose.yml   (service: bigquery)
.env.local
requirements.txt
```

Related: [10 Athena](../floci-aws-guides/10-athena.md). Projects that use this service are listed in the [README](README.md#projects).
