# 11 — Project 1: Event-Driven Ingest: Cloud Storage → Pub/Sub → BigQuery

> [Index](README.md) · [← 10 — Cloud Run](10-cloud-run.md) · [12 — Project 2: Order Queue: Cloud Tasks → Cloud Run Worker → Firestore →](12-project-order-queue.md)

---

## 1. Documentation

### Overview

Drop a CSV into a bucket; a notification lands on Pub/Sub; a consumer reads the file and loads rows into BigQuery. Wires **Cloud Storage + Pub/Sub + BigQuery**.

```
CSV upload ─▶ GCS bucket ─▶ Pub/Sub "file-events" ─▶ consumer ─▶ BigQuery demo.sales
```

### At a glance

| Item | Value |
|------|-------|
| Wires | Cloud Storage + Pub/Sub + BigQuery |
| Compose services | `gcs pubsub bigquery` |
| Script | `p1_ingest.py` (code is in Example 1; save it in `project/`) |
| Environment | `source .env.local` |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d gcs pubsub bigquery
source .env.local
python p1_ingest.py
```
(The emulators don't emit bucket notifications, so the uploader publishes the event itself — in real GCP, a bucket notification config does this for you.)

---

## 3. Examples

### Example 1 — `p1_ingest.py`

```python
import csv, io, json
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, pubsub_v1, storage

P = "demo-project"
gcs = storage.Client(project=P, credentials=AnonymousCredentials())
bq = bigquery.Client(project=P, credentials=AnonymousCredentials(),
                     client_options=ClientOptions(api_endpoint="http://localhost:9050"))
pub, sub = pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()

topic, sub_path = pub.topic_path(P, "file-events"), sub.subscription_path(P, "file-events-sub")

# --- setup ---
bucket = gcs.bucket("landing")
if not bucket.exists(): gcs.create_bucket("landing")
try: pub.create_topic(request={"name": topic})
except Exception: pass
try: sub.create_subscription(request={"name": sub_path, "topic": topic})
except Exception: pass
bq.create_table(bigquery.Table(f"{P}.demo.sales", schema=[
    bigquery.SchemaField("id", "INTEGER"), bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("amount", "FLOAT")]), exists_ok=True)

# --- producer: upload + announce ---
body = "id,name,amount\n1,ana,10.5\n2,ben,20\n3,cara,7.25\n"
bucket.blob("incoming/sales.csv").upload_from_string(body, content_type="text/csv")
pub.publish(topic, json.dumps({"bucket": "landing", "name": "incoming/sales.csv"}).encode()).result()

# --- consumer: read event, load file, insert rows ---
resp = sub.pull(request={"subscription": sub_path, "max_messages": 10}, timeout=10)
for m in resp.received_messages:
    evt = json.loads(m.message.data)
    text = gcs.bucket(evt["bucket"]).blob(evt["name"]).download_as_text()
    rows = [{"id": int(r["id"]), "name": r["name"], "amount": float(r["amount"])}
            for r in csv.DictReader(io.StringIO(text))]
    errs = bq.insert_rows_json(f"{P}.demo.sales", rows)
    print("loaded", len(rows), "rows, errors:", errs)
    sub.acknowledge(request={"subscription": sub_path, "ack_ids": [m.ack_id]})

for r in bq.query(f"SELECT COUNT(*) AS n, SUM(amount) AS total FROM `{P}.demo.sales`").result():
    print(dict(r))
```

---

## Reference

### Extend it

- Move the consumer into a Cloud Function (see guide 09).
- Add a dead-letter topic for files that fail to parse.
- Real GCP: `gcloud storage buckets notifications create gs://landing --topic=file-events`.

Guides for the services this project wires: see [README](README.md#guide-index).
