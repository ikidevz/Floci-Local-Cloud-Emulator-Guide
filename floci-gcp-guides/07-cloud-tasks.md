# 07 — Cloud Tasks

> [Index](README.md) · [← 06 — Spanner](06-spanner.md) · [08 — Cloud SQL (PostgreSQL) →](08-cloud-sql.md)

---

## 1. Documentation

### Overview

Cloud Tasks is a managed queue for **asynchronous HTTP work**. You enqueue a task; Cloud Tasks calls your HTTP endpoint with retries, rate limits, and optional delays. Similar to SQS + a push worker.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | cloud-tasks-emulator |
| AWS equivalent | SQS |
| Compose service | `tasks` |
| Host port(s) | `8123` |
| Environment variable | `CLOUD_TASKS_EMULATOR_HOST` |
| Python package | `google-cloud-tasks` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d tasks      # emulator on localhost:8123
```
Flow: create queue → create task (URL + body) → emulator POSTs to the URL.

---

## 3. Examples

### Example 1 — Core flow

```python
import grpc, json
from google.cloud import tasks_v2
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport

channel = grpc.insecure_channel("localhost:8123")
client = tasks_v2.CloudTasksClient(transport=CloudTasksGrpcTransport(channel=channel))

parent = client.common_location_path("demo-project", "us-central1")
queue = client.create_queue(parent=parent, queue={"name": f"{parent}/queues/emails"})

task = {
    "http_request": {
        "http_method": tasks_v2.HttpMethod.POST,
        "url": "http://host.docker.internal:8081/work",
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"to": "ana@example.com"}).encode(),
    }
}
print(client.create_task(parent=queue.name, task=task).name)
```
Tiny worker to receive it:
```python
# worker.py   ->  functions-framework --target work --port 8081
import functions_framework
@functions_framework.http
def work(request):
    print("got task:", request.get_json())
    return "ok", 200          # non-2xx => Cloud Tasks retries
```
Delay a task: add `"schedule_time": <protobuf Timestamp>` to the task dict.
### Example 2 — Terraform (real GCP)

```hcl
resource "google_cloud_tasks_queue" "emails" {
  name = "emails"
  location = "us-central1"
  rate_limits { max_dispatches_per_second = 5 }
  retry_config { max_attempts = 5 }
}
```

---

## Reference

Project files (shared by every GCP guide):

```
docker-compose.yml   (service: tasks)
.env.local
requirements.txt
```

Related: [14 SQS + DLQ](../floci-aws-guides/14-sqs-dlq.md). Projects that use this service are listed in the [README](README.md#projects).
