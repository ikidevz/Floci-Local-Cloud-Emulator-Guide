# 09 — Cloud Functions

> [Index](README.md) · [← 08 — Cloud SQL (PostgreSQL)](08-cloud-sql.md) · [10 — Cloud Run →](10-cloud-run.md)

---

## 1. Documentation

### Overview

Cloud Functions runs small pieces of code in response to HTTP requests or events (Pub/Sub, Cloud Storage), with no servers to manage. The GCP counterpart of Lambda.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | functions-framework |
| AWS equivalent | Lambda |
| Compose service | — (runs locally with the framework) |
| Host port(s) | chosen on the command line (`8081`, `8082` in the examples) |
| Environment variable | none |
| Python package | `functions-framework` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

Locally, the **Functions Framework** runs your function exactly like production:
```bash
pip install functions-framework
functions-framework --target hello --port 8081
curl "localhost:8081?name=Ana"
```

---

## 3. Examples

### Example 1 — HTTP function (`main.py`)

```python
import functions_framework

@functions_framework.http
def hello(request):
    name = request.args.get("name", "world")
    return {"message": f"hello {name}"}
```
### Example 2 — Pub/Sub-triggered function (CloudEvent)

```python
import base64, functions_framework

@functions_framework.cloud_event
def on_message(event):
    data = base64.b64decode(event.data["message"]["data"]).decode()
    print("received:", data)
```
Run: `functions-framework --target on_message --signature-type cloudevent --port 8082`

Test it by sending a CloudEvent locally:
```bash
curl localhost:8082 -X POST -H "Content-Type: application/json" \
  -H "ce-id: 1" -H "ce-specversion: 1.0" -H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
  -H "ce-source: //pubsub" \
  -d '{"message":{"data":"aGVsbG8="}}'
```
### Example 3 — Terraform (real GCP, 2nd gen)

```hcl
resource "google_cloudfunctions2_function" "hello" {
  name     = "hello"
  location = "us-central1"
  build_config {
    runtime = "python312"
    entry_point = "hello"
    source { storage_source { bucket = google_storage_bucket.src.name object = google_storage_bucket_object.zip.name } }
  }
  service_config { max_instance_count = 1 }
}
```

---

## Reference

Project files (shared by every GCP guide):

```
docker-compose.yml   (service: none)
.env.local
requirements.txt
```

Related: [02 Lambda](../floci-aws-guides/02-lambda.md). Projects that use this service are listed in the [README](README.md#projects).
