# 00 — Setup & Shared Foundation

> [Index](README.md) · [01 — Cloud Storage →](01-cloud-storage.md)

Everything the other guides rely on: the shared emulator stack, the environment variables, the Python dependencies and the client conventions.

---

## 1. Documentation

### Overview

- Floci itself emulates AWS. For Google Cloud this set uses the official or community emulator for each service, all started from one `project/docker-compose.yml`.
- No cloud account, credentials or bill. Project id is `demo-project`.
- Every guide reads its emulator address from an environment variable set by `project/.env.local`.

### At a glance

| Item | Value |
|------|-------|
| Project id | `demo-project` |
| Cloud Storage | `fake-gcs-server` on `4443` |
| Pub/Sub | gcloud emulator on `8085` |
| Firestore | gcloud emulator on `8080` |
| Bigtable | gcloud emulator on `8086` |
| Spanner | emulator on `9010` (gRPC) and `9020` (REST) |
| BigQuery | `goccy/bigquery-emulator` on `9050` (HTTP) and `9060` (gRPC) |
| Cloud Tasks | `cloud-tasks-emulator` on `8123` |
| Cloud SQL | Postgres 16 on `5432`, db `app`, password `postgres` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

### Conventions

- Clients for Pub/Sub, Firestore, Bigtable, Spanner and Cloud Tasks pick up their emulator from the `*_EMULATOR_HOST` variables.
- The Cloud Storage and BigQuery examples pass `AnonymousCredentials()` explicitly, and BigQuery points at the emulator with `ClientOptions(api_endpoint="http://localhost:9050")`.
- Start only what you need: `docker compose up -d <service>` (compose service names are in each guide's *At a glance* table).

### What was verified

These files do not record any test run. Treat all code as unverified until you run it against the emulators, and send back any failure.

---

## 2. Basic Usage

### Step 1: Start the emulators

```bash
cd project
docker compose up -d                 # everything, or name services: docker compose up -d pubsub bigquery
```

### Step 2: Point your tools at them

```bash
source .env.local
env | grep EMULATOR_HOST             # confirm the variables are set
```

### Step 3: Install the Python dependencies

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Step 4: Verify

```bash
docker compose ps                    # every service you started should be "running"
```

---

## 3. Examples

### Example 1 — Full `docker-compose.yml`

`project/docker-compose.yml`

```yaml
services:
  gcs:
    image: fsouza/fake-gcs-server
    command: ["-scheme", "http", "-port", "4443", "-public-host", "localhost:4443"]
    ports: ["4443:4443"]
  pubsub:
    image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
    command: gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=demo-project
    ports: ["8085:8085"]
  firestore:
    image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
    command: gcloud emulators firestore start --host-port=0.0.0.0:8080 --project=demo-project
    ports: ["8080:8080"]
  bigtable:
    image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
    command: gcloud beta emulators bigtable start --host-port=0.0.0.0:8086 --project=demo-project
    ports: ["8086:8086"]
  spanner:
    image: gcr.io/cloud-spanner-emulator/emulator
    ports: ["9010:9010", "9020:9020"]
  bigquery:
    image: ghcr.io/goccy/bigquery-emulator:latest
    command: ["--project=demo-project", "--dataset=demo"]
    ports: ["9050:9050", "9060:9060"]
  tasks:
    image: ghcr.io/aertje/cloud-tasks-emulator:latest
    command: ["-host", "0.0.0.0", "-port", "8123"]
    ports: ["8123:8123"]
  postgres:
    image: postgres:16
    environment: {POSTGRES_PASSWORD: postgres, POSTGRES_DB: app}
    ports: ["5432:5432"]
```

### Example 2 — Environment

`project/.env.local`

```bash
export GOOGLE_CLOUD_PROJECT=demo-project
export STORAGE_EMULATOR_HOST=http://localhost:4443
export PUBSUB_EMULATOR_HOST=localhost:8085
export FIRESTORE_EMULATOR_HOST=localhost:8080
export BIGTABLE_EMULATOR_HOST=localhost:8086
export SPANNER_EMULATOR_HOST=localhost:9010
export BIGQUERY_EMULATOR_HOST=http://localhost:9050
export CLOUD_TASKS_EMULATOR_HOST=localhost:8123
```

### Example 3 — Dependencies

`project/requirements.txt`

```
google-cloud-storage
google-cloud-pubsub
google-cloud-bigquery
google-cloud-firestore
google-cloud-bigtable
google-cloud-spanner
google-cloud-tasks
psycopg2-binary
functions-framework
flask
requests
```

### Example 4 — Terraform

Terraform examples in the service guides target real GCP, not the emulators.

---

## Reference

Project files:

```
project/docker-compose.yml
project/.env.local
project/requirements.txt
```
