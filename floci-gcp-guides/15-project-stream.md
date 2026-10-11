# 15 — Project 5: Stream: Pub/Sub → Windowed Aggregation → Firestore + BigQuery

> [Index](README.md) · [← 14 — Project 4: Batch: Cloud SQL → Cloud Storage → BigQuery (nightly ETL)](14-project-batch-etl.md) · [16 — Project 6: CDC: Cloud SQL → Pub/Sub → BigQuery changelog + Firestore replica →](16-project-cdc.md)

---

## 1. Documentation

### Overview

A **streaming** consumer uses Pub/Sub StreamingPull to receive click events continuously, counts them in 5-second **tumbling windows**, writes the live counts to Firestore (for a dashboard), and appends window results to BigQuery (history). Wires **Pub/Sub + Firestore + BigQuery**.

```
producer ─▶ Pub/Sub "clicks" ─▶ streaming consumer ─[5s window]─▶ Firestore live/<page>  +  BigQuery click_windows
```

### At a glance

| Item | Value |
|------|-------|
| Wires | Pub/Sub + Firestore + BigQuery |
| Compose services | `pubsub firestore bigquery` |
| Script | `p5_stream.py` (code is in Example 1; save it in `project/`) |
| Environment | `source .env.local` |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d pubsub firestore bigquery
source .env.local
python p5_stream.py consumer     # terminal 1 (runs until Ctrl+C)
python p5_stream.py producer     # terminal 2
```

---

## 3. Examples

### Example 1 — `p5_stream.py`

```python
import json, random, sys, threading, time, datetime as dt
from collections import Counter
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, firestore, pubsub_v1

P, WINDOW = "demo-project", 5
pub, sub = pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()
topic, sub_path = pub.topic_path(P, "clicks"), sub.subscription_path(P, "clicks-sub")

def setup():
    try: pub.create_topic(request={"name": topic})
    except Exception: pass
    try: sub.create_subscription(request={"name": sub_path, "topic": topic})
    except Exception: pass

def producer():
    setup()
    while True:
        evt = {"page": random.choice(["/home", "/cart", "/pricing"]), "ts": time.time()}
        pub.publish(topic, json.dumps(evt).encode())
        time.sleep(0.2)

def consumer():
    setup()
    db = firestore.Client(project=P)
    bq = bigquery.Client(project=P, credentials=AnonymousCredentials(),
                         client_options=ClientOptions(api_endpoint="http://localhost:9050"))
    bq.create_table(bigquery.Table(f"{P}.demo.click_windows", schema=[
        bigquery.SchemaField("window_start", "TIMESTAMP"), bigquery.SchemaField("page", "STRING"),
        bigquery.SchemaField("clicks", "INTEGER")]), exists_ok=True)

    lock, counts = threading.Lock(), Counter()

    def on_message(m):
        page = json.loads(m.data)["page"]
        with lock: counts[page] += 1
        m.ack()

    future = sub.subscribe(sub_path, callback=on_message)       # StreamingPull
    print("consuming... Ctrl+C to stop")
    try:
        while True:
            time.sleep(WINDOW)
            with lock:
                snapshot = dict(counts); counts.clear()
            start = dt.datetime.now(dt.timezone.utc).replace(microsecond=0) - dt.timedelta(seconds=WINDOW)
            for page, n in snapshot.items():
                db.collection("live").document(page.strip("/") or "root").set(
                    {"page": page, "clicks_last_window": n, "updated": firestore.SERVER_TIMESTAMP})
            if snapshot:
                bq.insert_rows_json(f"{P}.demo.click_windows",
                    [{"window_start": start.isoformat(), "page": p, "clicks": n} for p, n in snapshot.items()])
            print(start.time(), snapshot)
    except KeyboardInterrupt:
        future.cancel()

if __name__ == "__main__":
    {"producer": producer, "consumer": consumer}[sys.argv[1]]()
```
Check the live view: `db.collection("live").stream()` or query `SELECT page, SUM(clicks) FROM demo.click_windows GROUP BY page`.

---

## Reference

### Extend it

- Use **event time** (`ts`) instead of arrival time, and allow late events (a "watermark").
- Handle failures: don't `ack()` until the window write succeeds (at-least-once), and dedupe by message ID.
- On real GCP, replace the hand-rolled window with Dataflow (Apache Beam) `FixedWindows`.

Guides for the services this project wires: see [README](README.md#guide-index).
