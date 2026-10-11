# 07 — Service Bus

> [Index](README.md) · [← 06 — Event Hubs](06-event-hubs.md) · [08 — Event Grid →](08-event-grid.md)

Fan-out of results to the right audience: one message is published once and each subscription's SQL filter decides who receives it.

---

## 1. Documentation

### Overview

- **Azure Service Bus (topics and subscriptions)** — Fan-out of results to the right audience: one message is published once and each subscription's SQL filter decides who receives it.

**Where the three pipelines use it**

| Pipeline | Role | Pattern that matters |
|---|---|---|
| 2 Streaming | topic `fleet-alerts`: `pager` (CRITICAL) and `ops` (WARNING) | `message_id = alert_id` so a replayed batch re-sends the same id |
| 3 DQ gate | topic `dq-results`: `ops`, `compliance`, `downstream` | one result, three audiences; a file can be accepted *and* critical |

### At a glance

| Item | Value |
|------|-------|
| Python packages | `azure-servicebus==7.*` |
| Dev / test packages | `pytest`, `sqlglot` |
| Floci AZ ports published | `4577`, `5673`, `5674` |
| Docker socket | required |
| Floci settings in compose | `FLOCI_AZ_SERVICES_SERVICE_BUS_MOCKED`, `FLOCI_AZ_SERVICES_SERVICE_BUS_START_ON_BOOT` |
| Project folder | `project/07-service-bus/` |
| Tests | 15 |
| Verified against | `Config.json` limits; every subscription filter **executed** on sample messages; mutation-checked |

### How it behaves on Floci

- **Endpoints:** management `http://localhost:4577/<account>-servicebus`; AMQP 1.0 data plane on `5673` (AMQPS `5674`) via an Artemis sidecar.
- **The default is MOCKED.** With `MOCKED=true` (the default) the management API answers but **nothing can be sent or received**. Set `MOCKED=false`, expose the AMQP ports and set `START_ON_BOOT=true`.
- **Topology:** pre-provision with a declarative `Config.json` mounted at `/ServiceBus_Emulator/ConfigFiles/Config.json` (the official emulator's format). Entities can also be created through the management plane.
- **Filters:** `SqlFilter` (SQL92 over application properties and `sys.*`), `CorrelationFilter`, True/False. Filters on `MessageId`, `To`, `ReplyTo`, `ContentType` are **rejected (HTTP 400)**. Rule *actions* are stored but not applied. Multiple rules combine as OR.
- **Limits:** `LockDuration` <= `PT5M`, `MaxDeliveryCount` 1-2000, duplicate-detection window `PT20S`-`P7D`; `ForwardTo` is not emulated.

Source: Floci AZ docs, `https://floci.io/floci-az/services/`.

### Key concepts

What the minimal project proves, and where in the code:

| Pattern | Why the pipelines need it | Where |
|---|---|---|
| Topology as code | Consumers attach to entities that must already exist | `Config.json` |
| Deterministic `message_id` | Replays can be absorbed by duplicate detection | `messages.py` |
| Everything a filter needs travels in application properties | SQL filters see properties, not the body | `messages.py` |
| The routing itself | The pager must never get a WARNING; compliance must see a CRITICAL even on an accepted file | tests (filters executed) + `check.py` (on the broker) |

### Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Sends hang or fail; management calls work | `MOCKED` is still `true` | `FLOCI_AZ_SERVICES_SERVICE_BUS_MOCKED=false` + publish 5673 |
| Nothing listens on 5673 at startup | Namespace starts on first entity otherwise | `..._START_ON_BOOT=true` |
| A subscription receives everything | Its rule was skipped (invalid) and `$Default` remained | Check the Floci log for an `ERROR` on that entity |
| HTTP 400 creating a rule | Filter on `MessageId`/`To`/`ReplyTo`/`ContentType` | Filter on application properties or `Subject`/`CorrelationId` |

### What was verified

**Ran and passed (build sandbox):**

- 15 tests: `Config.json` respects Floci's documented limits and avoids the filter fields Floci rejects; every filter is valid SQL and only references properties our message builders actually set; **each subscription filter executed** (via SQLite as a SQL92 evaluator) against sample messages - 3 alert cases and 6 DQ cases, including accepted-and-critical fanning out to two audiences; message ids are deterministic.
- Mutation-checked: widening the pager filter makes the routing test fail.

**Not run:**

- Any contact with a broker - `bus.py` and `check.py` compile but never ran. **The `Rules` schema inside `Config.json` (`FilterType: Sql` / `SqlFilter.SqlExpression`) follows the official emulator's format from memory and was not run against Floci.** If Floci skips a rule it logs an `ERROR` and the subscription keeps its default accept-everything rule - `check.py` would then fail loudly on routing.
- Duplicate detection: `RequiresDuplicateDetection` is set, but Floci's docs do not claim to enforce it. `check.py` sends one alert twice and prints an INFO line either way.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md) read once. Commands start from this guides folder (`floci-azure-guides/`); the project lives in `project/07-service-bus/`.

### Project structure

```text
project/07-service-bus/
  Config.json
  bus.py
  check.py
  docker-compose.yml
  messages.py
  requirements-dev.txt
  requirements.txt
  tests/
    test_topology_and_messages.py
```

### Run it

```bash
cd project/07-service-bus && pip install -r requirements-dev.txt
pytest tests -v                          # no broker needed
docker compose up -d                     # mounts Config.json; MOCKED=false; Artemis sidecar needs the Docker socket
python check.py                          # publish -> drain every subscription -> assert routing
```

---

## 3. Examples

Every file below is the exact content of `project/07-service-bus/`.

### Example 1 — `docker-compose.yml`

````yaml
services:
  floci-az:
    image: floci/floci-az:latest
    ports:
      - "4577:4577"          # management plane: /<acct>-servicebus
      - "5673:5673"          # AMQP 1.0 data plane (Artemis sidecar)
      - "5674:5674"          # AMQPS
    volumes:
      - ./Config.json:/ServiceBus_Emulator/ConfigFiles/Config.json:ro     # declarative topology, applied at startup
      - /var/run/docker.sock:/var/run/docker.sock                          # REQUIRED: Artemis runs as a sidecar container
    environment:
      FLOCI_AZ_SERVICES_SERVICE_BUS_MOCKED: "false"          # DEFAULT IS TRUE: management calls answer, but nothing can be sent or received
      FLOCI_AZ_SERVICES_SERVICE_BUS_START_ON_BOOT: "true"    # otherwise nothing listens on :5673 until an entity is created
````
### Example 2 — `Config.json`

````json
{
  "UserConfig": {
    "Namespaces": [
      {
        "Name": "sbemulatorns",
        "Topics": [
          {
            "Name": "fleet-alerts",
            "Properties": { "DefaultMessageTimeToLive": "P1D", "DuplicateDetectionHistoryTimeWindow": "PT1H", "RequiresDuplicateDetection": true },
            "Subscriptions": [
              {
                "Name": "pager",
                "Properties": { "DefaultMessageTimeToLive": "P1D", "LockDuration": "PT1M", "MaxDeliveryCount": 5 },
                "Rules": [ { "Name": "critical-only", "Properties": { "FilterType": "Sql", "SqlFilter": { "SqlExpression": "severity = 'CRITICAL'" } } } ]
              },
              {
                "Name": "ops",
                "Properties": { "DefaultMessageTimeToLive": "P1D", "LockDuration": "PT1M", "MaxDeliveryCount": 5 },
                "Rules": [ { "Name": "warnings", "Properties": { "FilterType": "Sql", "SqlFilter": { "SqlExpression": "severity = 'WARNING'" } } } ]
              }
            ]
          },
          {
            "Name": "dq-results",
            "Properties": { "DefaultMessageTimeToLive": "P1D", "DuplicateDetectionHistoryTimeWindow": "PT1H", "RequiresDuplicateDetection": true },
            "Subscriptions": [
              {
                "Name": "ops",
                "Properties": { "DefaultMessageTimeToLive": "P1D", "LockDuration": "PT1M", "MaxDeliveryCount": 5 },
                "Rules": [ { "Name": "filter", "Properties": { "FilterType": "Sql", "SqlFilter": { "SqlExpression": "route = 'QUARANTINED' OR route = 'DUPLICATE'" } } } ]
              },
              {
                "Name": "compliance",
                "Properties": { "DefaultMessageTimeToLive": "P1D", "LockDuration": "PT1M", "MaxDeliveryCount": 5 },
                "Rules": [ { "Name": "filter", "Properties": { "FilterType": "Sql", "SqlFilter": { "SqlExpression": "severity = 'CRITICAL'" } } } ]
              },
              {
                "Name": "downstream",
                "Properties": { "DefaultMessageTimeToLive": "P1D", "LockDuration": "PT1M", "MaxDeliveryCount": 5 },
                "Rules": [ { "Name": "filter", "Properties": { "FilterType": "Sql", "SqlFilter": { "SqlExpression": "route = 'ACCEPTED' OR route = 'PARTIAL'" } } } ]
              }
            ]
          }
        ]
      }
    ],
    "Logging": { "Type": "File" }
  }
}
````
### Example 3 — `bus.py`

````python
"""Publish to a topic / drain a subscription. UseDevelopmentEmulator=true => plain AMQP on :5673, key ignored."""
import os

from azure.servicebus import ServiceBusClient


def conn_str():
    return os.environ.get("SERVICEBUS_CONNECTION_STRING",
                          "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=devkey;UseDevelopmentEmulator=true;")


def publish(topic, messages, conn=None):
    with ServiceBusClient.from_connection_string(conn or conn_str()) as c, c.get_topic_sender(topic) as s:
        s.send_messages(list(messages))


def drain(topic, subscription, max_wait=5, conn=None):
    """Receive and COMPLETE everything currently on a subscription -> [(message_id, application_properties)]."""
    out = []
    with ServiceBusClient.from_connection_string(conn or conn_str()) as c, c.get_subscription_receiver(topic, subscription, max_wait_time=max_wait) as r:
        for m in r:
            out.append((m.message_id, {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in (m.application_properties or {}).items()}))
            r.complete_message(m)
    return out
````
### Example 4 — `check.py`

````python
"""python check.py   (Floci AZ up with Config.json mounted)  -> publish, drain every subscription, assert the routing."""
import sys

import bus
import messages as M

ALERTS = [dict(alert_id="a-crit-1", vehicle_id="V-0024", rule="TEMP_CRITICAL", severity="CRITICAL"),
          dict(alert_id="a-warn-1", vehicle_id="V-0011", rule="DOOR_OPEN_TOO_LONG", severity="WARNING"),
          dict(alert_id="a-warn-2", vehicle_id="V-0001", rule="SENSOR_GAP", severity="WARNING")]
RESULTS = [dict(message_id="f1:aaaa", route="QUARANTINED", severity="ERROR"), dict(message_id="f2:bbbb", route="ACCEPTED", severity="INFO"),
           dict(message_id="f3:cccc", route="PARTIAL", severity="CRITICAL"), dict(message_id="f4:dddd", route="DUPLICATE", severity="WARNING")]
EXPECT = {("fleet-alerts", "pager"): {"a-crit-1"}, ("fleet-alerts", "ops"): {"a-warn-1", "a-warn-2"},
          ("dq-results", "ops"): {"f1:aaaa", "f4:dddd"}, ("dq-results", "compliance"): {"f3:cccc"}, ("dq-results", "downstream"): {"f2:bbbb", "f3:cccc"}}


def main():
    bus.publish("fleet-alerts", [M.alert_message(a) for a in ALERTS] + [M.alert_message(ALERTS[0])])        # the CRITICAL alert is sent TWICE (replay)
    bus.publish("dq-results", [M.result_message(r) for r in RESULTS])
    ok = True
    for (topic, sub), want in EXPECT.items():
        ids = [m for m, _ in bus.drain(topic, sub)]
        good = set(ids) == want
        ok &= good
        print("PASS" if good else "FAIL", f"{topic}/{sub}: got {sorted(set(ids))} want {sorted(want)}")
        if ids.count("a-crit-1") > 1:
            print(f"  INFO: duplicate delivered {ids.count('a-crit-1')}x - the emulator does not enforce duplicate detection (real Azure would deliver once)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
````
### Example 5 — `messages.py`

````python
"""Message builders for the two routing patterns. Two rules make a topic safe under at-least-once delivery:
  * message_id is DETERMINISTIC (a replayed batch re-sends the SAME id, so duplicate detection can absorb it)
  * everything a subscription filters on travels in application_properties (that is what SQL filters see)."""
import json

from azure.servicebus import ServiceBusMessage


def alert_message(alert: dict) -> ServiceBusMessage:
    """Streaming pipeline -> topic fleet-alerts. alert_id = hash(vehicle, rule, trigger time)."""
    return ServiceBusMessage(json.dumps(alert), message_id=alert["alert_id"], subject=alert["rule"],
                             application_properties={"severity": alert["severity"], "rule": alert["rule"], "vehicle_id": alert["vehicle_id"]})


def result_message(result: dict) -> ServiceBusMessage:
    """DQ gate -> topic dq-results. message_id = '<file name>:<content hash prefix>'."""
    return ServiceBusMessage(json.dumps(result), message_id=result["message_id"], subject=result["route"],
                             application_properties={"route": result["route"], "severity": result["severity"]})
````
### Example 6 — `requirements-dev.txt`

````text
azure-servicebus==7.*
pytest
sqlglot
````
### Example 7 — `requirements.txt`

````text
azure-servicebus==7.*
````
### Example 8 — `tests/test_topology_and_messages.py`

````python
import json
import pathlib
import re
import sqlite3

import pytest
import sqlglot
from sqlglot import exp

import messages as M

ROOT = pathlib.Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "Config.json").read_text())["UserConfig"]["Namespaces"][0]
SUBS = [(t["Name"], s["Name"], s["Rules"][0]["Properties"]["SqlFilter"]["SqlExpression"]) for t in CFG["Topics"] for s in t["Subscriptions"]]


def iso_seconds(d):                                           # PT20S / PT1M / PT1H / P1D ... -> seconds
    m = re.fullmatch(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?", d)
    days, h, mi, s = (int(x or 0) for x in m.groups())
    return days * 86400 + h * 3600 + mi * 60 + s


def test_config_respects_floci_limits():                     # limits documented by Floci AZ (it silently SKIPS invalid entities)
    for t in CFG["Topics"]:
        p = t["Properties"]
        assert 20 <= iso_seconds(p["DuplicateDetectionHistoryTimeWindow"]) <= 7 * 86400 and p["RequiresDuplicateDetection"] is True
        for s in t["Subscriptions"]:
            sp = s["Properties"]
            assert iso_seconds(sp["LockDuration"]) <= 300 and 1 <= sp["MaxDeliveryCount"] <= 2000
            assert "ForwardTo" not in sp and "ForwardDeadLetteredMessagesTo" not in sp       # not emulated: Floci only logs a warning
            assert len(s["Rules"]) == 1 and s["Rules"][0]["Properties"]["FilterType"] == "Sql"


def test_filters_never_use_fields_the_emulator_rejects():     # MessageId/To/ReplyTo/ContentType filters are HTTP 400 in Floci AZ
    for _, _, expr in SUBS:
        assert not re.search(r"messageid|replyto|contenttype|\bto\b", expr, re.I)


@pytest.mark.parametrize("topic,sub,expr", SUBS)
def test_filter_is_valid_sql_and_uses_only_properties_our_messages_carry(topic, sub, expr):
    tree = sqlglot.parse_one(f"SELECT 1 WHERE {expr}")
    used = {c.name for c in tree.find_all(exp.Column)}
    carried = set(M.alert_message(dict(alert_id="x", vehicle_id="v", rule="r", severity="s")).application_properties) if topic == "fleet-alerts" \
        else set(M.result_message(dict(message_id="x", route="r", severity="s")).application_properties)
    assert used and used <= carried, (used, carried)


def route(topic, props):
    """Evaluate every subscription filter of a topic against message properties (SQLite as the SQL92 evaluator)."""
    hits = set()
    cols = ", ".join(f"'{v}' AS {k}" for k, v in props.items())
    for t, sub, expr in SUBS:
        if t == topic and sqlite3.connect(":memory:").execute(f"SELECT 1 FROM (SELECT {cols}) WHERE {expr}").fetchone():
            hits.add(sub)
    return hits


def props_of(msg):
    return dict(msg.application_properties)


def test_alert_routing():
    mk = lambda sev: props_of(M.alert_message(dict(alert_id="x", vehicle_id="v", rule="r", severity=sev)))
    assert route("fleet-alerts", mk("CRITICAL")) == {"pager"}
    assert route("fleet-alerts", mk("WARNING")) == {"ops"}
    assert route("fleet-alerts", mk("INFO")) == set()


@pytest.mark.parametrize("r,s,want", [("QUARANTINED", "ERROR", {"ops"}), ("DUPLICATE", "WARNING", {"ops"}), ("ACCEPTED", "INFO", {"downstream"}),
                                      ("PARTIAL", "WARNING", {"downstream"}), ("ACCEPTED", "CRITICAL", {"downstream", "compliance"}),   # accepted AND critical
                                      ("QUARANTINED", "CRITICAL", {"ops", "compliance"})])
def test_dq_routing_fans_out_one_result_to_three_audiences(r, s, want):
    assert route("dq-results", props_of(M.result_message(dict(message_id="x", route=r, severity=s)))) == want


def test_message_id_is_deterministic_so_replays_can_be_absorbed():
    a = dict(alert_id="abc123", vehicle_id="V-1", rule="TEMP_CRITICAL", severity="CRITICAL")
    assert M.alert_message(a).message_id == M.alert_message(dict(a)).message_id == "abc123"
    assert M.alert_message(a).subject == "TEMP_CRITICAL" and json.loads(str(M.alert_message(a)))["vehicle_id"] == "V-1"
    assert M.result_message(dict(message_id="f:9", route="ACCEPTED", severity="INFO")).message_id == "f:9"
````
### Example 9 — On real Azure (Terraform and auth)

- Terraform: `azurerm_servicebus_namespace` (Standard - topics need it) + `azurerm_servicebus_topic` (`requires_duplicate_detection`) + subscriptions + `azurerm_servicebus_subscription_rule` (`SqlFilter`).
- Duplicate detection genuinely absorbs a re-sent `message_id` within the window.
- Auth: *Azure Service Bus Data Sender* for the producer identity; connect with `DefaultAzureCredential` and the namespace FQDN.

---

## Reference

Project files:

```
Config.json
bus.py
check.py
docker-compose.yml
messages.py
requirements-dev.txt
requirements.txt
tests/test_topology_and_messages.py
```

AWS counterpart: [06 SNS](../floci-aws-guides/06-sns.md), [14 SQS + DLQ](../floci-aws-guides/14-sqs-dlq.md).
