# 06 — Event Hubs

> [Index](README.md) · [← 05 — Key Vault](05-key-vault.md) · [07 — Service Bus →](07-service-bus.md)

The ingestion front door of the streaming pipeline: sensor events keyed by vehicle so each vehicle's events stay ordered on one partition.

---

## 1. Documentation

### Overview

- **Azure Event Hubs** — The ingestion front door of the streaming pipeline: sensor events keyed by vehicle so each vehicle's events stay ordered on one partition.

**Where the three pipelines use it**

| Pipeline | Role | Pattern that matters |
|---|---|---|
| 2 Streaming | hub `telemetry`, 4 partitions, partition key = `vehicle_id` | ordering per key; a bridge replays the missing Event Hub *trigger* |

### At a glance

| Item | Value |
|------|-------|
| Python packages | `azure-eventhub==5.*`, `azure-eventhub-checkpointstoreblob==1.*` |
| Dev / test packages | `pytest` |
| Floci AZ ports published | `4577`, `5672` |
| Docker socket | required |
| Floci settings in compose | `FLOCI_AZ_SERVICES_EVENT_HUB_ENABLED`, `FLOCI_AZ_SERVICES_EVENT_HUB_ENTITIES` |
| Project folder | `project/06-event-hubs/` |
| Tests | 8 |
| Verified against | unit tests with fakes; SDK source inspected |

### How it behaves on Floci

- **Endpoint:** AMQP 1.0 on `localhost:5672` (Apache Artemis sidecar; Kafka on 9093 is optional). Health: `/<account>-eventhub/health`.
- **Hubs are pre-configured**, not created at runtime: `FLOCI_AZ_SERVICES_EVENT_HUB_ENTITIES=telemetry:4` (`name:partitions`). The default is only `eh1:4`.
- **Connection string:** `Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=devkey;UseDevelopmentEmulator=true;` (plain AMQP, key ignored).
- **Not emulated:** the Event Hub *trigger* for Functions, Schema Registry, Capture.

Source: Floci AZ docs, `https://floci.io/floci-az/services/`.

### Key concepts

What the minimal project proves, and where in the code:

| Pattern | Why the pipelines need it | Where |
|---|---|---|
| Per-key ordering | State-based rules (debounce, escalation) are meaningless if a vehicle's events interleave | `producer.plan_batches` |
| Batching within limits; unparseable events still get a key | Malformed events must reach the dead-letter path, not crash the producer | `plan_batches`, `key_of` |
| Bridge: checkpoint only after the function returned 200 | At-least-once with no loss | `bridge.make_handler` |
| End-to-end ordering + partition affinity on the broker | The real guarantee | `check.py` |

### Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Hub `telemetry` does not exist | Entities come from config, default `eh1:4` | Set `..._EVENT_HUB_ENTITIES=telemetry:4` |
| The bridge re-reads the whole hub on every start | No checkpoint store, so `update_checkpoint` is a silent no-op | Set `CHECKPOINT_CONTAINER` (Blob-backed) *or* rely on idempotent processing |
| A failed batch is not re-delivered while the bridge runs | The SDK keeps consuming after a handler gives up | The retry loop is in-process; a restart replays from the last checkpoint |
| Connection refused on 5672 | Port not published or sidecar not up | Publish `5672:5672`; check `/<account>-eventhub/health` |

### What was verified

**Ran and passed (build sandbox):**

- 8 unit tests: key order preserved across chunks and the 100-event limit, a batch never mixes keys, truncated JSON still gets a key, checkpoint advances only on success, retries with backoff before giving up, empty batch ignored, an unreachable function counts as failure.
- Read the installed SDK source: **`update_checkpoint()` without a checkpoint store only logs a warning.** See Pitfalls.

**Not run:**

- Any contact with a broker - `producer.send`, `bridge.run` and `check.py` compile but never ran (no Artemis/Docker in the build sandbox). The optional `BlobCheckpointStore` against Floci Blob is untested; it relies on blob metadata and conditional writes.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md) read once. Commands start from this guides folder (`floci-azure-guides/`); the project lives in `project/06-event-hubs/`.

### Project structure

```text
project/06-event-hubs/
  bridge.py
  check.py
  docker-compose.yml
  hub.py
  producer.py
  requirements-dev.txt
  requirements.txt
  tests/
    test_hub.py
```

### Run it

```bash
cd project/06-event-hubs && pip install -r requirements-dev.txt
pytest tests -v                          # no broker needed
docker compose up -d                     # starts the Artemis sidecar: needs the Docker socket
python check.py                          # sends 24 interleaved events for 3 vehicles, proves per-vehicle order + one partition each
# the bridge needs a function that returns 200, e.g. project 03's `run`:
FUNCTION_URL=http://localhost:4577/devstoreaccount1-functions/api/demo-app/run python bridge.py
```

---

## 3. Examples

Every file below is the exact content of `project/06-event-hubs/`.

### Example 1 — `docker-compose.yml`

````yaml
services:
  floci-az:
    image: floci/floci-az:latest
    ports:
      - "4577:4577"          # HTTP: management, health  (/<acct>-eventhub/health)
      - "5672:5672"          # Event Hubs AMQP 1.0 (Artemis sidecar) - the data plane
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock    # REQUIRED: the Artemis broker is started as a sidecar container
    environment:
      FLOCI_AZ_SERVICES_EVENT_HUB_ENABLED: "true"
      FLOCI_AZ_SERVICES_EVENT_HUB_ENTITIES: "telemetry:4"      # name:partitions. Hubs are PRE-CONFIGURED here, not created at runtime
      # Only the $Default consumer group is guaranteed; extra groups come from config (see the Floci AZ Event Hubs page).
````
### Example 2 — `bridge.py`

````python
"""Floci AZ can only invoke Functions over HTTP and has no Event Hub TRIGGER, so this small bridge plays the trigger:
it reads batches from the hub and POSTs them to the function. Real Azure: delete the bridge, use the Event Hub trigger.

Contract: advance the checkpoint ONLY after the function answered 200. Caveat that bites: update_checkpoint() is a NO-OP
(warning only) unless the consumer has a checkpoint store. Set CHECKPOINT_CONTAINER to use a Blob-backed one; without it the
bridge starts from the beginning of the hub every run - fine ONLY because the processor is idempotent (at-least-once)."""
import json
import os
import time
import urllib.error
import urllib.request

import hub

FUNCTION_URL = os.environ.get("FUNCTION_URL", "http://localhost:4577/devstoreaccount1-functions/api/coldchain-fn/process_batch")


def post_events(bodies, url=None, timeout=120):
    req = urllib.request.Request(url or FUNCTION_URL, method="POST", data=json.dumps({"events": bodies}).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status == 200
    except (urllib.error.URLError, OSError):
        return False                                                  # HTTPError (500) is a URLError: non-200 == "not processed"


def make_handler(post, attempts=3, sleep=time.sleep, log=print):
    """-> on_event_batch(partition_context, events). Checkpoint only on success; retry a few times first."""
    def on_event_batch(pc, events):
        if not events:
            return
        bodies = [e.body_as_str() for e in events]
        for i in range(attempts):
            if post(bodies):
                pc.update_checkpoint(events[-1])                      # pass the event: batch mode has no implicit 'last event' guarantee
                return
            sleep(2 ** i)
        log(f"partition {pc.partition_id}: {len(events)} events NOT processed; checkpoint NOT advanced (they replay on restart)")
    return on_event_batch


def run(conn=None, post=post_events, stop_after=None):
    from azure.eventhub import EventHubConsumerClient
    store = None
    if os.environ.get("CHECKPOINT_CONTAINER"):
        from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
        store = BlobCheckpointStore.from_connection_string(os.environ["STORAGE_CONNECTION_STRING"], os.environ["CHECKPOINT_CONTAINER"])
    c = EventHubConsumerClient.from_connection_string(conn or hub.conn_str(), consumer_group=hub.CONSUMER_GROUP, eventhub_name=hub.HUB, checkpoint_store=store)
    if stop_after:
        import threading
        threading.Timer(stop_after, c.close).start()
    with c:
        c.receive_batch(on_event_batch=make_handler(post), max_batch_size=100, max_wait_time=5, starting_position="-1")


if __name__ == "__main__":
    run()
````
### Example 3 — `check.py`

````python
"""python check.py   (Floci AZ running)  -> send events for 3 vehicles, read them back, prove per-key ordering and partition affinity."""
import json
import sys
import threading

import hub
import producer

VEHICLES, PER = ["V-0001", "V-0002", "V-0003"], 8


def main():
    from azure.eventhub import EventHubConsumerClient
    events = [(v, json.dumps({"vehicle_id": v, "seq": i})) for i in range(PER) for v in VEHICLES]       # interleaved on the wire
    print("sent", producer.send(events))
    seen, lock = [], threading.Lock()

    def on_event(pc, e):
        if e is None:
            return
        d = json.loads(e.body_as_str())
        with lock:
            seen.append((pc.partition_id, d["vehicle_id"], d["seq"]))
    c = EventHubConsumerClient.from_connection_string(hub.conn_str(), consumer_group=hub.CONSUMER_GROUP, eventhub_name=hub.HUB)
    threading.Timer(20, c.close).start()
    with c:
        c.receive(on_event=on_event, starting_position="-1", max_wait_time=None)
    ok = True
    for v in VEHICLES:
        mine = [(p, s) for p, vid, s in seen if vid == v]
        parts = {p for p, _ in mine}
        in_order = [s for _, s in mine][-PER:] == list(range(PER))
        print("PASS" if len(parts) == 1 and in_order else "FAIL", f"{v}: partitions={sorted(parts)} in-order={in_order} events={len(mine)}")
        ok &= len(parts) == 1 and in_order
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
````
### Example 4 — `hub.py`

````python
"""Event Hubs connection. UseDevelopmentEmulator=true => plain AMQP (no TLS) on :5672; the key is ignored by the emulator."""
import os

HUB = os.environ.get("EVENTHUB_NAME", "telemetry")
CONSUMER_GROUP = os.environ.get("EVENTHUB_CONSUMER_GROUP", "$Default")


def conn_str():
    return os.environ.get("EVENTHUB_CONNECTION_STRING",
                          "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=devkey;UseDevelopmentEmulator=true;")
````
### Example 5 — `producer.py`

````python
"""Producer side: partition KEY = vehicle_id, so every event of one vehicle lands on one partition IN ORDER.
That ordering is what makes per-vehicle state (debouncing, escalation, de-dup) possible downstream."""
import json
from collections import OrderedDict

import hub


def plan_batches(events, max_batch=100):
    """events: [(partition_key, body_str)] in send order -> [(partition_key, [bodies])] with each key's order preserved
    and no batch larger than max_batch. Pure, so the ordering guarantee is unit-testable without a broker."""
    by_key = OrderedDict()
    for key, body in events:
        by_key.setdefault(key, []).append(body)
    return [(k, bodies[i:i + max_batch]) for k, bodies in by_key.items() for i in range(0, len(bodies), max_batch)]


def key_of(raw):
    """Partition key for a raw event. Unparseable events still need one, or they could not be sent (they are dead-lettered downstream)."""
    try:
        return json.loads(raw)["vehicle_id"]
    except Exception:  # noqa: BLE001
        return "malformed"


def send(events, conn=None, hub_name=None):
    """events: [(key, body)]. One send per planned batch; same key -> same partition."""
    from azure.eventhub import EventData, EventHubProducerClient
    sent = 0
    with EventHubProducerClient.from_connection_string(conn or hub.conn_str(), eventhub_name=hub_name or hub.HUB) as p:
        for key, bodies in plan_batches(events):
            b = p.create_batch(partition_key=key)
            for body in bodies:
                b.add(EventData(body))
            p.send_batch(b)
            sent += len(bodies)
    return sent
````
### Example 6 — `requirements-dev.txt`

````text
azure-eventhub==5.*
pytest
````
### Example 7 — `requirements.txt`

````text
azure-eventhub==5.*
azure-eventhub-checkpointstoreblob==1.*     # optional: durable checkpoints in Blob (without it update_checkpoint is a no-op)
````
### Example 8 — `tests/test_hub.py`

````python
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import bridge  # noqa: E402
import hub  # noqa: E402
import producer  # noqa: E402


def test_batches_keep_each_keys_order_and_size_limit():
    events = [(k, f"{k}-{i}") for i in range(250) for k in ("A", "B")]          # interleaved
    batches = producer.plan_batches(events, max_batch=100)
    assert all(len(b) <= 100 for _, b in batches)
    for k in ("A", "B"):
        flat = [x for key, b in batches if key == k for x in b]
        assert flat == [f"{k}-{i}" for i in range(250)]                          # order preserved across chunks
    assert {key for key, _ in batches} == {"A", "B"}                             # a batch never mixes keys (one partition key per batch)


def test_key_of_handles_garbage():
    assert producer.key_of(json.dumps({"vehicle_id": "V-1"})) == "V-1"
    assert producer.key_of('{"vehicle_id": "V-1", "te') == "malformed"          # truncated JSON still gets a key
    assert producer.key_of("{}") == "malformed"


def test_connection_string_uses_development_emulator():
    assert "UseDevelopmentEmulator=true" in hub.conn_str() and "Endpoint=sb://localhost;" in hub.conn_str()


class Ev:
    def __init__(self, b): self.b = b
    def body_as_str(self): return self.b


class PC:
    partition_id = "0"
    def __init__(self): self.checkpointed = None
    def update_checkpoint(self, event=None): self.checkpointed = event


def test_checkpoint_advances_only_after_200():
    pc, evs = PC(), [Ev("a"), Ev("b")]
    bridge.make_handler(lambda bodies: True)(pc, evs)
    assert pc.checkpointed is evs[-1]


def test_no_checkpoint_when_function_keeps_failing_and_it_retries_first():
    pc, calls, logs = PC(), [], []
    def post(bodies):
        calls.append(bodies); return False
    bridge.make_handler(post, attempts=3, sleep=lambda s: None, log=logs.append)(pc, [Ev("a")])
    assert pc.checkpointed is None and len(calls) == 3 and "NOT advanced" in logs[0]


def test_recovers_on_second_attempt():
    pc, n = PC(), {"i": 0}
    def post(bodies):
        n["i"] += 1; return n["i"] >= 2
    bridge.make_handler(post, attempts=3, sleep=lambda s: None)(pc, [Ev("a")])
    assert pc.checkpointed is not None


def test_empty_batch_is_ignored():
    pc = PC()
    bridge.make_handler(lambda b: (_ for _ in ()).throw(AssertionError("must not call")))(pc, [])
    assert pc.checkpointed is None


def test_post_events_treats_unreachable_as_failure():
    assert bridge.post_events(["x"], url="http://127.0.0.1:9/none", timeout=1) is False
````
### Example 9 — On real Azure (Terraform and auth)

- Terraform: `azurerm_eventhub_namespace` (Standard) + `azurerm_eventhub` (`partition_count = 4`) + a consumer group.
- Delete the bridge: use the Event Hub trigger with an identity-based connection (`EventHubConnection__fullyQualifiedNamespace`) and the *Azure Event Hubs Data Receiver* role.
- The trigger checkpoints to the function's storage account for you.

---

## Reference

Project files:

```
bridge.py
check.py
docker-compose.yml
hub.py
producer.py
requirements-dev.txt
requirements.txt
tests/test_hub.py
```

AWS counterpart: [11 Kinesis Data Streams](../floci-aws-guides/11-kinesis-data-streams.md).
Related: [03 Azure Functions](03-azure-functions.md).
