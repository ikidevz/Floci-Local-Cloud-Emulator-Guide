# 05 — Bigtable

> [Index](README.md) · [← 04 — Firestore](04-firestore.md) · [06 — Spanner →](06-spanner.md)

---

## 1. Documentation

### Overview

Bigtable is a wide-column NoSQL database built for huge, low-latency workloads (time series, IoT, ad tech). Rows are addressed by a single **row key**; values live in **column families**.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | gcloud bigtable emulator |
| AWS equivalent | Keyspaces / DynamoDB |
| Compose service | `bigtable` |
| Host port(s) | `8086` |
| Environment variable | `BIGTABLE_EMULATOR_HOST` |
| Python package | `google-cloud-bigtable` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d bigtable
source .env.local            # sets BIGTABLE_EMULATOR_HOST
```
Key tip: design the **row key** for your read pattern, e.g. `sensor#<id>#<reverse_timestamp>`.

---

## 3. Examples

### Example 1 — Core flow

```python
import datetime
from google.cloud import bigtable
from google.cloud.bigtable import column_family, row_filters

client = bigtable.Client(project="demo-project", admin=True)
instance = client.instance("demo-instance")
table = instance.table("metrics")

if not table.exists():
    table.create(column_families={"m": column_family.MaxVersionsGCRule(1)})

# write
row = table.direct_row(b"sensor#1#20260101T000000")
row.set_cell("m", b"temp", b"21.5", timestamp=datetime.datetime.utcnow())
row.commit()

# read one row
r = table.read_row(b"sensor#1#20260101T000000")
print(r.cells["m"][b"temp"][0].value)

# prefix scan
from google.cloud.bigtable.row_set import RowSet
rs = RowSet(); rs.add_row_range_from_keys(b"sensor#1#", b"sensor#1#~")
for row in table.read_rows(row_set=rs):
    print(row.row_key, row.cells["m"][b"temp"][0].value)
```
### Example 2 — Terraform (real GCP)

```hcl
resource "google_bigtable_instance" "i" {
  name = "demo-instance"
  cluster { cluster_id = "c1" zone = "us-central1-b" num_nodes = 1 storage_type = "SSD" }
}
resource "google_bigtable_table" "t" {
  name          = "metrics"
  instance_name = google_bigtable_instance.i.name
  column_family { family = "m" }
}
```

---

## Reference

Project files (shared by every GCP guide):

```
docker-compose.yml   (service: bigtable)
.env.local
requirements.txt
```

Related: [13 DynamoDB](../floci-aws-guides/13-dynamodb.md). Projects that use this service are listed in the [README](README.md#projects).
