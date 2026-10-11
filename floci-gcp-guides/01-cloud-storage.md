# 01 — Cloud Storage

> [Index](README.md) · [← 00 — Setup & Shared Foundation](00-setup-floci.md) · [02 — Pub/Sub →](02-pubsub.md)

---

## 1. Documentation

### Overview

Cloud Storage is Google Cloud's object store: you put files ("objects") into containers ("buckets") and fetch them by name. It's the GCP counterpart of S3, used for data lakes, backups, static assets, and pipeline landing zones.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | fake-gcs-server |
| AWS equivalent | S3 |
| Compose service | `gcs` |
| Host port(s) | `4443` |
| Environment variable | `STORAGE_EMULATOR_HOST` |
| Python package | `google-cloud-storage` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d gcs
source .env.local            # sets STORAGE_EMULATOR_HOST
```
Core ideas: **bucket** (container) → **blob** (object). Upload, download, list, delete.

---

## 3. Examples

### Example 1 — Core flow

```python
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

client = storage.Client(project="demo-project", credentials=AnonymousCredentials())

# create bucket
bucket = client.create_bucket("demo-bucket")

# upload
bucket.blob("raw/hello.txt").upload_from_string("hello gcs")
bucket.blob("raw/data.csv").upload_from_string("id,name\n1,ana\n2,ben\n", content_type="text/csv")

# list
for b in client.list_blobs("demo-bucket", prefix="raw/"):
    print(b.name, b.size)

# download
print(bucket.blob("raw/hello.txt").download_as_text())

# delete
bucket.blob("raw/hello.txt").delete()
```
### Example 2 — Terraform (real GCP)

```hcl
resource "google_storage_bucket" "demo" {
  name          = "demo-bucket-${var.project_id}"
  location      = "US"
  force_destroy = true
  uniform_bucket_level_access = true
}
```

---

## Reference

Project files (shared by every GCP guide):

```
docker-compose.yml   (service: gcs)
.env.local
requirements.txt
```

Related: [01 S3](../floci-aws-guides/01-s3.md). Projects that use this service are listed in the [README](README.md#projects).
