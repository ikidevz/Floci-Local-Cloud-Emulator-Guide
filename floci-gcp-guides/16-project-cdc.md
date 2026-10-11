# 16 — Project 6: CDC: Cloud SQL → Pub/Sub → BigQuery changelog + Firestore replica

> [Index](README.md) · [← 15 — Project 5: Stream: Pub/Sub → Windowed Aggregation → Firestore + BigQuery](15-project-stream.md)

---

## 1. Documentation

### Overview

**Change Data Capture** detects inserts, updates, and deletes in a source database and streams them downstream. Here a poller diffs Postgres against its last snapshot (log-free CDC), publishes change events to Pub/Sub, and a consumer (a) appends every change to a BigQuery **changelog** and (b) keeps a Firestore replica of the current state. Wires **Cloud SQL + Pub/Sub + BigQuery + Firestore**.

```
Postgres customers ─▶ diff poller ─▶ Pub/Sub "cdc" {op, id, row} ─┬▶ BigQuery customers_changelog (history)
                                                                   └▶ Firestore customers/<id> (current state)
```
Real GCP equivalent: **Datastream** reads the Postgres WAL and delivers to BigQuery/Cloud Storage with no polling.

### At a glance

| Item | Value |
|------|-------|
| Wires | Cloud SQL + Pub/Sub + BigQuery + Firestore |
| Compose services | `postgres pubsub firestore bigquery` |
| Script | `p6_cdc.py` (code is in Example 1; save it in `project/`) |
| Environment | `source .env.local` |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d postgres pubsub firestore bigquery
source .env.local
python p6_cdc.py setup     # tables + topic
python p6_cdc.py poll      # terminal 1: detect and publish changes
python p6_cdc.py consume   # terminal 2: apply changes
python p6_cdc.py mutate    # terminal 3: insert/update/delete rows to watch it flow
```

---

## 3. Examples

### Example 1 — `p6_cdc.py`

```python
import hashlib, json, sys, time
import psycopg2
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, firestore, pubsub_v1

P = "demo-project"
pg = psycopg2.connect(host="localhost", dbname="app", user="postgres", password="postgres")
pg.autocommit = True
pub, sub = pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()
topic, sub_path = pub.topic_path(P, "cdc"), sub.subscription_path(P, "cdc-sub")

def bq_client():
    return bigquery.Client(project=P, credentials=AnonymousCredentials(),
                           client_options=ClientOptions(api_endpoint="http://localhost:9050"))

def setup():
    with pg.cursor() as c:
        c.execute("CREATE TABLE IF NOT EXISTS customers(id INT PRIMARY KEY, name TEXT, city TEXT)")
    try: pub.create_topic(request={"name": topic})
    except Exception: pass
    try: sub.create_subscription(request={"name": sub_path, "topic": topic})
    except Exception: pass
    bq_client().create_table(bigquery.Table(f"{P}.demo.customers_changelog", schema=[
        bigquery.SchemaField("seq", "INTEGER"), bigquery.SchemaField("op", "STRING"),
        bigquery.SchemaField("id", "INTEGER"), bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("city", "STRING")]), exists_ok=True)

def fingerprint(row):
    return hashlib.md5(json.dumps(row, sort_keys=True).encode()).hexdigest()

def poll(interval=2):
    snapshot, seq = {}, int(time.time())          # id -> (hash, row)
    while True:
        with pg.cursor() as c:
            c.execute("SELECT id, name, city FROM customers")
            current = {r[0]: {"id": r[0], "name": r[1], "city": r[2]} for r in c.fetchall()}
        events = []
        for i, row in current.items():
            if i not in snapshot:                      events.append(("insert", row))
            elif snapshot[i][0] != fingerprint(row):   events.append(("update", row))
        for i, (_, row) in snapshot.items():
            if i not in current:                       events.append(("delete", row))
        for op, row in events:
            seq += 1
            pub.publish(topic, json.dumps({"seq": seq, "op": op, **row}).encode()).result()
            print("change:", op, row)
        snapshot = {i: (fingerprint(r), r) for i, r in current.items()}
        time.sleep(interval)

def consume():
    db, bq = firestore.Client(project=P), bq_client()
    while True:
        resp = sub.pull(request={"subscription": sub_path, "max_messages": 50}, timeout=10)
        for m in resp.received_messages:
            e = json.loads(m.message.data)
            bq.insert_rows_json(f"{P}.demo.customers_changelog", [e])          # history
            ref = db.collection("customers").document(str(e["id"]))           # current state
            ref.delete() if e["op"] == "delete" else ref.set({k: e[k] for k in ("id", "name", "city")})
            sub.acknowledge(request={"subscription": sub_path, "ack_ids": [m.ack_id]})
            print("applied:", e["op"], e["id"])
        time.sleep(1)

def mutate():
    with pg.cursor() as c:
        c.execute("INSERT INTO customers VALUES (1,'Ana','Davao') ON CONFLICT (id) DO NOTHING")
        c.execute("INSERT INTO customers VALUES (2,'Ben','Manila') ON CONFLICT (id) DO NOTHING")
    time.sleep(3)
    with pg.cursor() as c: c.execute("UPDATE customers SET city='Cebu' WHERE id=1")
    time.sleep(3)
    with pg.cursor() as c: c.execute("DELETE FROM customers WHERE id=2")

if __name__ == "__main__":
    {"setup": setup, "poll": poll, "consume": consume, "mutate": mutate}[sys.argv[1]]()
```
### Example 2 — Current state from the changelog (latest event per id, ignoring deletes)

```sql
SELECT * EXCEPT(rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY seq DESC) AS rn
  FROM `demo-project.demo.customers_changelog`)
WHERE rn = 1 AND op != 'delete';
```

---

## Reference

### Extend it

- Swap polling for an `updated_at` watermark column (cheaper) or Postgres logical replication.
- Consumers should be idempotent: ordering by `seq` makes replays safe.
- Real GCP: Datastream (Cloud SQL → BigQuery) replaces the poller + consumer entirely.

Guides for the services this project wires: see [README](README.md#guide-index).
