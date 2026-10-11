# 04 — Firestore

> [Index](README.md) · [← 03 — BigQuery](03-bigquery.md) · [05 — Bigtable →](05-bigtable.md)

---

## 1. Documentation

### Overview

Firestore is a serverless NoSQL document database. Data lives in **collections** of **documents** (JSON-like). It's the closest GCP match to DynamoDB, with richer queries.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | gcloud firestore emulator |
| AWS equivalent | DynamoDB |
| Compose service | `firestore` |
| Host port(s) | `8080` |
| Environment variable | `FIRESTORE_EMULATOR_HOST` |
| Python package | `google-cloud-firestore` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d firestore
source .env.local            # sets FIRESTORE_EMULATOR_HOST
```

---

## 3. Examples

### Example 1 — Core flow

```python
from google.cloud import firestore

db = firestore.Client(project="demo-project")

# create / set
db.collection("users").document("u1").set({"name": "Ana", "age": 30, "tags": ["admin"]})

# add with auto id
_, ref = db.collection("users").add({"name": "Ben", "age": 25})

# read
print(db.collection("users").document("u1").get().to_dict())

# query
for d in db.collection("users").where("age", ">=", 26).stream():
    print(d.id, d.to_dict())

# update + transaction
db.collection("users").document("u1").update({"age": firestore.Increment(1)})

@firestore.transactional
def bump(tx, ref):
    snap = ref.get(transaction=tx)
    tx.update(ref, {"age": snap.get("age") + 1})

bump(db.transaction(), db.collection("users").document("u1"))

# delete
db.collection("users").document("u1").delete()
```
### Example 2 — Terraform (real GCP)

```hcl
resource "google_firestore_database" "db" {
  name        = "(default)"
  location_id = "nam5"
  type        = "FIRESTORE_NATIVE"
}
```

---

## Reference

Project files (shared by every GCP guide):

```
docker-compose.yml   (service: firestore)
.env.local
requirements.txt
```

Related: [13 DynamoDB](../floci-aws-guides/13-dynamodb.md). Projects that use this service are listed in the [README](README.md#projects).
