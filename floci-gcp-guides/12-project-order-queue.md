# 12 — Project 2: Order Queue: Cloud Tasks → Cloud Run Worker → Firestore

> [Index](README.md) · [← 11 — Project 1: Event-Driven Ingest: Cloud Storage → Pub/Sub → BigQuery](11-project-event-ingest.md) · [13 — Project 3: Telemetry Pipeline: Pub/Sub → Bigtable → BigQuery →](13-project-telemetry.md)

---

## 1. Documentation

### Overview

An API accepts orders, enqueues a Cloud Task per order, and a worker processes it and updates the order's status in Firestore. Wires **Cloud Tasks + Cloud Run (container/HTTP) + Firestore**.

```
POST /orders ─▶ Firestore (status=pending) ─▶ Cloud Tasks ─▶ POST /work ─▶ Firestore (status=done)
```

### At a glance

| Item | Value |
|------|-------|
| Wires | Cloud Tasks + Cloud Run (container/HTTP) + Firestore |
| Compose services | `tasks firestore` |
| Script | `p2_app.py` (code is in Example 1; save it in `project/`) |
| Environment | `source .env.local` |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d tasks firestore
source .env.local
python p2_app.py            # API + worker on :8081
curl -X POST localhost:8081/orders -H 'Content-Type: application/json' -d '{"item":"book","qty":2}'
```

---

## 3. Examples

### Example 1 — `p2_app.py`

```python
import json, uuid
import grpc
from flask import Flask, jsonify, request
from google.cloud import firestore, tasks_v2
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport

P, REGION, QUEUE = "demo-project", "us-central1", "orders"
db = firestore.Client(project=P)
tasks = tasks_v2.CloudTasksClient(
    transport=CloudTasksGrpcTransport(channel=grpc.insecure_channel("localhost:8123")))
parent = tasks.common_location_path(P, REGION)
queue_path = f"{parent}/queues/{QUEUE}"
try: tasks.create_queue(parent=parent, queue={"name": queue_path})
except Exception: pass

app = Flask(__name__)

@app.post("/orders")
def create_order():
    oid = str(uuid.uuid4())
    db.collection("orders").document(oid).set({**request.get_json(), "status": "pending"})
    tasks.create_task(parent=queue_path, task={"http_request": {
        "http_method": tasks_v2.HttpMethod.POST,
        "url": "http://host.docker.internal:8081/work",   # emulator runs in Docker
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"order_id": oid}).encode()}})
    return jsonify(order_id=oid, status="pending"), 202

@app.post("/work")
def work():
    oid = request.get_json()["order_id"]
    ref = db.collection("orders").document(oid)
    ref.update({"status": "processing"})
    # ... do real work here (charge card, send email, etc.) ...
    ref.update({"status": "done", "processed_at": firestore.SERVER_TIMESTAMP})
    return "ok", 200            # non-2xx => retried

@app.get("/orders/<oid>")
def get_order(oid):
    return jsonify(db.collection("orders").document(oid).get().to_dict())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081)
```
Check: `curl localhost:8081/orders/<order_id>` → `status: done`.

---

## Reference

### Extend it

- Containerize with the Dockerfile from guide 10 and deploy to Cloud Run.
- Make `/work` idempotent (skip if status is already `done`) since tasks can be delivered twice.
- Add `schedule_time` to delay processing.

Guides for the services this project wires: see [README](README.md#guide-index).
