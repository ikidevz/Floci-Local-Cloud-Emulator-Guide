# 08 — Event Grid

> [Index](README.md) · [← 07 — Service Bus](07-service-bus.md)

Turns "a file landed" into a call: a blob appears in `inbox/`, Event Grid POSTs an event to the gate Function.

---

## 1. Documentation

### Overview

- **Azure Event Grid (custom topic + webhook)** — Turns "a file landed" into a call: a blob appears in `inbox/`, Event Grid POSTs an event to the gate Function.

**Where the three pipelines use it**

| Pipeline | Role | Pattern that matters |
|---|---|---|
| 3 DQ gate | `BlobCreated` for `inbox/` -> webhook = the gate Function | validation handshake, subject filter, at-least-once delivery, retry until 2xx |

### At a glance

| Item | Value |
|------|-------|
| Python packages | `pytest` |
| Floci AZ ports published | `4577` |
| Docker socket | not required |
| Project folder | `project/08-event-grid/` |
| Tests | 8 |
| Verified against | unit tests + a **live local HTTP server** |

### How it behaves on Floci

- **Endpoints:** ARM paths `Microsoft.EventGrid/topics/...` and `.../eventSubscriptions/...`; publish at `http://localhost:4577/<topic>-eventgrid/api/events` (keyed on the **topic name**). HTTP only, no sidecar.
- **Emulated:** custom topics, `listKeys`, **webhook** subscriptions with `subjectBeginsWith`/`subjectEndsWith`/`includedEventTypes`, `retryPolicy`, publish in Event Grid and CloudEvents schemas, async delivery with exponential-backoff retry, the `SubscriptionValidationEvent` handshake.
- **Not emulated:** the **storage system topic** (so the project publishes `BlobCreated` itself), Service Bus / Event Hub / Function destinations, domains, `advancedFilters`. Dead-lettering is best-effort: after the last attempt the event is logged and dropped, not written to a blob.
- Floci calls the webhook **from inside its container**, so use `host.docker.internal`, not `localhost`.

Source: Floci AZ docs, `https://floci.io/floci-az/services/`.

### Key concepts

What the minimal project proves, and where in the code:

| Pattern | Why the pipelines need it | Where |
|---|---|---|
| Answer the validation handshake | A subscription is refused unless the endpoint echoes the code | `gate_events.handle_events` |
| Two-layer self-trigger guard | The gate's own outputs are blobs too; without both layers it triggers itself | `grid.subscription_body` + `handle_events` |
| Non-2xx on failure | That is what makes Event Grid retry | `hook_server` |
| Create topic, key, subscription; publish | The management and data planes end to end | `grid.py`, `provision.py`, `check.py` |

### Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Subscription creation fails | The webhook was not running or did not echo `validationCode` | Start `hook_server.py` first |
| Floci cannot reach the webhook | `localhost` from inside the container | `host.docker.internal` (the compose file adds the host-gateway mapping) |
| The gate triggers itself | Only one of the two guards present | Keep the `subjectBeginsWith` filter *and* the handler's subject check |
| An event is lost after the retries | Floci's dead-lettering is best-effort (logged, dropped) | Real Azure writes it to a dead-letter blob container |

### What was verified

**Ran and passed (build sandbox):**

- 8 tests: handshake echo, only inbox `BlobCreated` processed (accepted/reports zones, other event types and subject-less events ignored), nested names keep their path, the subscription filter matches the handler's guard, published events have the shape the handler expects, and a **real local HTTP server** answers the handshake, events, the CloudEvents `OPTIONS` probe and returns 500 on failure.

**Not run:**

- Every call to Floci (`provision.py`, `check.py` only compiled): topic/subscription creation and the delivery path. The `retryPolicy` field names follow Azure's ARM schema; Floci lists `retryPolicy` as supported but this was not run.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md) read once. Commands start from this guides folder (`floci-azure-guides/`); the project lives in `project/08-event-grid/`.

### Project structure

```text
project/08-event-grid/
  check.py
  docker-compose.yml
  gate_events.py
  grid.py
  hook_server.py
  provision.py
  requirements.txt
  tests/
    test_events.py
```

### Run it

```bash
cd project/08-event-grid && pip install -r requirements.txt
pytest tests -v                          # no emulator needed (includes a live local HTTP server)
docker compose up -d
python hook_server.py &                  # FIRST: creating the subscription triggers the handshake against it
python provision.py                      # topic + webhook subscription (-> http://host.docker.internal:8099/gate)
python check.py                          # publish 3 events; only the 2 inbox ones may arrive
```

---

## 3. Examples

Every file below is the exact content of `project/08-event-grid/`.

### Example 1 — `docker-compose.yml`

````yaml
services:
  floci-az:
    image: floci/floci-az:latest
    ports:
      - "4577:4577"          # ARM paths (topics, subscriptions) AND the publish endpoint /<topic>-eventgrid/api/events. HTTP only, no sidecar.
    # Floci calls your webhook FROM INSIDE the container: use host.docker.internal:<port>, not localhost.
    # On native Linux Docker, if invocations time out and you run UFW:  sudo ufw allow in on docker0
    extra_hosts:
      - "host.docker.internal:host-gateway"
````
### Example 2 — `check.py`

````python
"""python check.py   (Floci AZ + hook_server.py + provision.py done) -> publish 3 events, only the 2 inbox ones may arrive."""
import json
import sys
import time
import urllib.request

import grid

endpoint, key = grid.create_topic(), grid.list_key()          # idempotent: PUT returns the existing topic
grid.publish(endpoint, key, [grid.blob_created("inbox", "BAN_20261002_001.csv"), grid.blob_created("inbox", "BAN_20261002_002.csv"),
                             grid.blob_created("accepted", "BAN/dt=2026-10-02/BAN_20261002_001.csv")])      # an OUTPUT of the gate: must not come back
for _ in range(20):
    got = json.loads(urllib.request.urlopen("http://localhost:8099/received", timeout=5).read())
    if len(got["processed"]) >= 2:
        break
    time.sleep(1)
results = {"both inbox events delivered": sorted(got["processed"]) == ["BAN_20261002_001.csv", "BAN_20261002_002.csv"],
           "output-zone event filtered out before delivery": got["ignored"] == 0}
for k, v in results.items():
    print("PASS" if v else "FAIL", k)
sys.exit(0 if all(results.values()) else 1)
````
### Example 3 — `gate_events.py`

````python
"""The Event Grid webhook contract, as a pure function (this is what the DQ-gate Function does with every POST).
  1. answer the SubscriptionValidationEvent handshake by echoing the validationCode
  2. process BlobCreated events for the INBOX only. Our own outputs are blobs too: without this guard the gate would trigger itself.
  3. Event Grid delivers AT LEAST ONCE: `process` must be idempotent, and an exception must become a non-2xx so Event Grid retries."""
INBOX = "/blobServices/default/containers/inbox/blobs/"
VALIDATION = "Microsoft.EventGrid.SubscriptionValidationEvent"
BLOB_CREATED = "Microsoft.Storage.BlobCreated"


def handle_events(events, process):
    if events and events[0].get("eventType") == VALIDATION:
        return {"validationResponse": events[0]["data"]["validationCode"]}
    done, ignored = [], 0
    for e in events:
        subject = e.get("subject", "")
        if e.get("eventType") == BLOB_CREATED and subject.startswith(INBOX):
            done.append(process(subject[len(INBOX):]))
        else:
            ignored += 1
    return {"processed": done, "ignored": ignored}
````
### Example 4 — `grid.py`

````python
"""Floci AZ Event Grid, over plain HTTP (no SDK needed).
   ARM (management)  PUT  /subscriptions/<s>/resourceGroups/<rg>/providers/Microsoft.EventGrid/topics/<name>
                     POST .../topics/<name>/listKeys
                     PUT  .../topics/<name>/providers/Microsoft.EventGrid/eventSubscriptions/<sub>
   Data plane        POST /<topic>-eventgrid/api/events      (header aeg-sas-key; the URL is keyed on the TOPIC name)
Scope limits (Floci AZ): custom topics + WEBHOOK destinations only. NO storage system topic, so we publish the BlobCreated event ourselves."""
import json
import os
import time
import urllib.error
import urllib.request

BASE = os.environ.get("FLOCI_ENDPOINT", "http://localhost:4577")
API = "api-version=2025-02-15"
TOPIC, SUB = "ikipay-blobs", "inbox-to-gate"
TOPIC_ID = f"/subscriptions/dev/resourceGroups/rg/providers/Microsoft.EventGrid/topics/{TOPIC}"
INBOX_SUBJECT_PREFIX = "/blobServices/default/containers/inbox/"


def http(method, url, body=None, headers=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {}


def create_topic():
    code, body = http("PUT", f"{BASE}{TOPIC_ID}?{API}", {"location": "eastus", "properties": {"inputSchema": "EventGridSchema"}})
    assert code in (200, 201), (code, body)
    return body["properties"]["endpoint"]


def list_key():
    code, keys = http("POST", f"{BASE}{TOPIC_ID}/listKeys?{API}")
    assert code == 200, (code, keys)
    return keys["key1"]


def subscription_body(hook_url):
    return {"properties": {
        "destination": {"endpointType": "WebHook", "properties": {"endpointUrl": hook_url}},
        "filter": {"subjectBeginsWith": INBOX_SUBJECT_PREFIX, "includedEventTypes": ["Microsoft.Storage.BlobCreated"]},   # layer 1 of the self-trigger guard
        "retryPolicy": {"maxDeliveryAttempts": 10, "eventTimeToLiveInMinutes": 1440}}}


def create_subscription(hook_url):
    code, body = http("PUT", f"{BASE}{TOPIC_ID}/providers/Microsoft.EventGrid/eventSubscriptions/{SUB}?{API}", subscription_body(hook_url))
    assert code in (200, 201), (code, body)       # fails if the webhook did not echo the validation code: is hook_server.py running?
    return body


def blob_created(container, name):
    return {"id": f"evt-{container}-{name}", "eventType": "Microsoft.Storage.BlobCreated", "dataVersion": "1", "data": {"api": "PutBlob"},
            "subject": f"/blobServices/default/containers/{container}/blobs/{name}", "eventTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def publish(endpoint, key, events):
    code, body = http("POST", endpoint, events, {"aeg-sas-key": key})
    assert code in (200, 202), (code, body)
````
### Example 5 — `hook_server.py`

````python
"""python hook_server.py [port]  -> a tiny local webhook standing in for the gate Function. START IT BEFORE provision.py:
creating the subscription makes Floci AZ call this URL immediately with the validation handshake."""
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

from gate_events import handle_events

RECEIVED = {"processed": [], "ignored": 0}


def process(name):
    RECEIVED["processed"].append(name)                 # idempotent by construction: a set-like record of file names
    return name


class Hook(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, code, body, headers=None):
        raw = json.dumps(body).encode()
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_OPTIONS(self):                              # CloudEvents abuse-protection probe
        self._json(200, {}, {"WebHook-Allowed-Origin": self.headers.get("WebHook-Request-Origin", "*"), "Allow": "POST, OPTIONS"})

    def do_GET(self):
        self._json(200, RECEIVED)

    def do_POST(self):
        try:
            events = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"[]")
            out = handle_events(events, process)
            RECEIVED["ignored"] += out.get("ignored", 0)
            self._json(200, out)
        except Exception as e:                         # non-2xx => Event Grid retries (and eventually gives up): exactly what we want
            self._json(500, {"error": f"{type(e).__name__}: {e}"})


def make_server(port=8099):
    return HTTPServer(("0.0.0.0", port), Hook)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    print(f"webhook listening on :{port}  (POST /gate, GET /received)")
    make_server(port).serve_forever()
````
### Example 6 — `provision.py`

````python
"""python provision.py [hook_url]   (hook_server.py already running)
default hook_url = http://host.docker.internal:8099/gate   <- Floci runs in a container; it cannot reach your host as 'localhost'."""
import sys

import grid

hook = sys.argv[1] if len(sys.argv) > 1 else "http://host.docker.internal:8099/gate"
print("topic endpoint:", grid.create_topic())
grid.create_subscription(hook)
print(f"subscription '{grid.SUB}' -> {hook}   (filter: {grid.INBOX_SUBJECT_PREFIX}, BlobCreated)")
````
### Example 7 — `requirements.txt`

````text
# standard library only; pytest for the tests
pytest
````
### Example 8 — `tests/test_events.py`

````python
import json
import pathlib
import sys
import threading
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import gate_events as G  # noqa: E402
import grid  # noqa: E402
import hook_server  # noqa: E402


def ev(subject, typ=G.BLOB_CREATED):
    return {"id": "1", "eventType": typ, "subject": subject, "data": {}}


def test_validation_handshake_echoes_the_code():
    assert G.handle_events([{"eventType": G.VALIDATION, "data": {"validationCode": "abc-123"}}], lambda n: 1 / 0) == {"validationResponse": "abc-123"}


def test_only_inbox_blob_created_events_are_processed():
    seen = []
    out = G.handle_events([ev(G.INBOX + "a.csv"), ev("/blobServices/default/containers/accepted/blobs/a.csv"), ev("/blobServices/default/containers/reports/blobs/a.json"),
                           ev(G.INBOX + "b.csv", "Microsoft.Storage.BlobDeleted"), {"eventType": G.BLOB_CREATED}], lambda n: seen.append(n) or n)
    assert seen == ["a.csv"] and out == {"processed": ["a.csv"], "ignored": 4}      # layer 2 of the self-trigger guard


def test_nested_inbox_names_keep_their_path():
    assert G.handle_events([ev(G.INBOX + "BAN/2026/x.csv")], lambda n: n)["processed"] == ["BAN/2026/x.csv"]


def test_subscription_filter_matches_the_handlers_guard():
    f = grid.subscription_body("http://h/gate")["properties"]["filter"]
    assert f["subjectBeginsWith"] == G.INBOX.split("blobs/")[0] and f["includedEventTypes"] == [G.BLOB_CREATED]
    assert grid.subscription_body("http://h/gate")["properties"]["destination"]["endpointType"] == "WebHook"      # the only destination Floci supports


def test_published_event_matches_what_the_handler_expects():
    e = grid.blob_created("inbox", "x.csv")
    assert e["subject"] == G.INBOX + "x.csv" and e["eventType"] == G.BLOB_CREATED


def test_topic_endpoint_is_keyed_on_topic_name():
    assert grid.TOPIC_ID.endswith(f"/topics/{grid.TOPIC}")


@pytest.fixture
def server():
    hook_server.RECEIVED.update(processed=[], ignored=0)
    s = hook_server.make_server(0)
    threading.Thread(target=s.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{s.server_port}"
    s.shutdown()


def post(url, body):
    req = urllib.request.Request(url, method="POST", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_server_answers_handshake_events_and_cloudevents_probe(server):
    assert post(server + "/gate", [{"eventType": G.VALIDATION, "data": {"validationCode": "zz"}}]) == (200, {"validationResponse": "zz"})
    assert post(server + "/gate", [ev(G.INBOX + "a.csv"), ev("/blobServices/default/containers/accepted/blobs/x")]) == (200, {"processed": ["a.csv"], "ignored": 1})
    assert json.loads(urllib.request.urlopen(server + "/received").read()) == {"processed": ["a.csv"], "ignored": 1}
    r = urllib.request.urlopen(urllib.request.Request(server + "/gate", method="OPTIONS", headers={"WebHook-Request-Origin": "eventgrid.azure.net"}))
    assert r.status == 200 and r.headers["WebHook-Allowed-Origin"] == "eventgrid.azure.net"


def test_a_failure_is_a_500_so_event_grid_retries(server, monkeypatch):
    monkeypatch.setattr(hook_server, "process", lambda n: (_ for _ in ()).throw(RuntimeError("storage down")))
    code, body = post(server + "/gate", [ev(G.INBOX + "a.csv")])
    assert code == 500 and "storage down" in body["error"]
````
### Example 9 — On real Azure (Terraform and auth)

- Terraform: `azurerm_eventgrid_system_topic` on the storage account + `azurerm_eventgrid_system_topic_event_subscription` (`subject_filter`, `retry_policy`, `storage_blob_dead_letter_destination`). No custom topic and no manual publish.
- The webhook URL carries the function key (`?code=`); `REQUIRE_FUNCTION_KEY=1`.
- Real Azure also dead-letters to a blob container you can inspect.

---

## Reference

Project files:

```
check.py
docker-compose.yml
gate_events.py
grid.py
hook_server.py
provision.py
requirements.txt
tests/test_events.py
```
