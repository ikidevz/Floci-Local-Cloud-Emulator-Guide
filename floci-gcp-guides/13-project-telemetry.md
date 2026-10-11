# 13 — Project 3: Telemetry Pipeline: Pub/Sub → Bigtable → BigQuery

> [Index](README.md) · [← 12 — Project 2: Order Queue: Cloud Tasks → Cloud Run Worker → Firestore](12-project-order-queue.md) · [14 — Project 4: Batch: Cloud SQL → Cloud Storage → BigQuery (nightly ETL) →](14-project-batch-etl.md)

---

## 1. Documentation

### Overview

A simulator publishes sensor readings to Pub/Sub; a consumer writes them to Bigtable for fast recent-reads, and a batch step copies aggregates into BigQuery for analytics. Wires **Pub/Sub + Bigtable + BigQuery**.

```
simulator ─▶ Pub/Sub "telemetry" ─▶ consumer ─▶ Bigtable (hot, by sensor+time) ─▶ rollup ─▶ BigQuery
```

### At a glance

| Item | Value |
|------|-------|
| Wires | Pub/Sub + Bigtable + BigQuery |
| Compose services | `pubsub bigtable bigquery` |
| Script | `p3_telemetry.py` (code is in Example 1; save it in `project/`) |
| Environment | `source .env.local` |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d pubsub bigtable bigquery
source .env.local
python p3_telemetry.py
```

---

## 3. Examples

### Example 1 — `p3_telemetry.py`

```python
import json, random, time, datetime as dt
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, bigtable, pubsub_v1
from google.cloud.bigtable import column_family
from google.cloud.bigtable.row_set import RowSet

P = "demo-project"
pub, sub = pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()
topic, sub_path = pub.topic_path(P, "telemetry"), sub.subscription_path(P, "telemetry-sub")
try: pub.create_topic(request={"name": topic})
except Exception: pass
try: sub.create_subscription(request={"name": sub_path, "topic": topic})
except Exception: pass

bt = bigtable.Client(project=P, admin=True)
table = bt.instance("demo-instance").table("readings")
if not table.exists():
    table.create(column_families={"m": column_family.MaxVersionsGCRule(1)})

bq = bigquery.Client(project=P, credentials=AnonymousCredentials(),
                     client_options=ClientOptions(api_endpoint="http://localhost:9050"))
bq.create_table(bigquery.Table(f"{P}.demo.sensor_avg", schema=[
    bigquery.SchemaField("sensor", "STRING"), bigquery.SchemaField("avg_temp", "FLOAT"),
    bigquery.SchemaField("n", "INTEGER")]), exists_ok=True)

# 1) simulate
for i in range(30):
    msg = {"sensor": f"s{random.randint(1, 3)}", "temp": round(random.uniform(18, 30), 2)}
    pub.publish(topic, json.dumps(msg).encode()).result()

# 2) consume -> Bigtable (row key: sensor#<reverse ts> so newest sorts first)
resp = sub.pull(request={"subscription": sub_path, "max_messages": 100}, timeout=10)
for m in resp.received_messages:
    d = json.loads(m.message.data)
    rev = 9_999_999_999_999 - int(time.time() * 1000)
    row = table.direct_row(f"{d['sensor']}#{rev}#{m.message.message_id}".encode())
    row.set_cell("m", b"temp", str(d["temp"]).encode())
    row.commit()
if resp.received_messages:
    sub.acknowledge(request={"subscription": sub_path, "ack_ids": [m.ack_id for m in resp.received_messages]})

# 3) latest 5 readings for one sensor
rs = RowSet(); rs.add_row_range_from_keys(b"s1#", b"s1#~")
for r in table.read_rows(row_set=rs, limit=5):
    print(r.row_key.decode(), r.cells["m"][b"temp"][0].value.decode())

# 4) rollup -> BigQuery
agg = {}
for r in table.read_rows():
    s = r.row_key.decode().split("#")[0]
    agg.setdefault(s, []).append(float(r.cells["m"][b"temp"][0].value))
bq.insert_rows_json(f"{P}.demo.sensor_avg",
    [{"sensor": s, "avg_temp": round(sum(v) / len(v), 2), "n": len(v)} for s, v in agg.items()])
for r in bq.query(f"SELECT * FROM `{P}.demo.sensor_avg` ORDER BY sensor").result():
    print(dict(r))
```

---

## Reference

### Extend it

- Run the consumer as a long-lived Cloud Run service using streaming pull.
- Schedule the rollup with Cloud Scheduler → Cloud Function.
- Replace the in-memory rollup with a BigQuery query over exported data.

Guides for the services this project wires: see [README](README.md#guide-index).
