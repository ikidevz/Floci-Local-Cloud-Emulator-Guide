# 10 — Cloud Run

> [Index](README.md) · [← 09 — Cloud Functions](09-cloud-functions.md) · [11 — Project 1: Event-Driven Ingest: Cloud Storage → Pub/Sub → BigQuery →](11-project-event-ingest.md)

---

## 1. Documentation

### Overview

Cloud Run runs any **container** that listens on `$PORT` and scales it from zero. It suits APIs, workers, and websites. Roughly Fargate/App Runner on AWS.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | Docker |
| AWS equivalent | Fargate / App Runner |
| Compose service | — (plain `docker run`) |
| Host port(s) | `8080` in the example |
| Environment variable | none |
| Python package | `flask` (example app) |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

Since Cloud Run is just a container, local testing is plain Docker:
```bash
docker build -t demo-api .
docker run -p 8080:8080 -e PORT=8080 demo-api
```

---

## 3. Examples

### Example 1 — Core flow

`app.py`
```python
import os
from flask import Flask, jsonify
app = Flask(__name__)

@app.get("/")
def root():
    return jsonify(status="ok")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
```
`Dockerfile`
```dockerfile
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir flask gunicorn
COPY app.py .
CMD exec gunicorn --bind :$PORT --workers 1 app:app
```
Deploy to real GCP:
```bash
gcloud run deploy demo-api --source . --region us-central1 --allow-unauthenticated
```
### Example 2 — Terraform (real GCP)

```hcl
resource "google_cloud_run_v2_service" "api" {
  name     = "demo-api"
  location = "us-central1"
  template { containers { image = "us-docker.pkg.dev/${var.project_id}/repo/demo-api:latest" } }
}
resource "google_cloud_run_v2_service_iam_member" "public" {
  name = google_cloud_run_v2_service.api.name
  location = "us-central1"
  role = "roles/run.invoker"
  member = "allUsers"
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

Related: Projects that use this service are listed in the [README](README.md#projects).
