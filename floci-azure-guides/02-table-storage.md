# 02 — Table Storage

> [Index](README.md) · [← 01 — Blob Storage](01-blob-storage.md) · [03 — Azure Functions →](03-azure-functions.md)

Small key/value tables used for control state: the file ledger, the live fleet board, the audit trail and the atomic content-hash claim.

---

## 1. Documentation

### Overview

- **Azure Table Storage** — Small key/value tables used for control state: the file ledger, the live fleet board, the audit trail and the atomic content-hash claim.

**Where the three pipelines use it**

| Pipeline | Role | Pattern that matters |
|---|---|---|
| 1 Batch ETL | `etlledger`: one row per file (`LOADED`/`TRANSFORMED`, ETag) and per run (`SUCCEEDED`/`FAILED`) | upsert + point read; ledger key is the file *name* |
| 2 Streaming | `fleetstate`: one hot row per vehicle (PartitionKey = depot) | read row + **ETag**, write back only `IfNotModified` |
| 3 DQ gate | `dqaudit`: `file` rows (audit) and `hash` rows (claim) | `create_entity` as an **atomic claim**: first writer wins |

### At a glance

| Item | Value |
|------|-------|
| Python packages | `azure-data-tables==12.*`, `pytest` |
| Floci AZ ports published | `4577` |
| Docker socket | not required |
| Project folder | `project/02-table-storage/` |
| Tests | 7 |
| Verified against | **Azurite** (real Table REST API: ETags, atomic create) |

### How it behaves on Floci

- **Endpoint:** `http://localhost:4577/<account>-table` - note the **`-table` suffix**. It is *not* the blob path.
- **Emulated:** create/delete/list tables, insert/get/update/upsert/delete, `$filter` (equality, numeric comparison, and/or/not), `$select`, pagination, **ETag/If-Match** (stale ETag rejected), `$batch` transactions.
- **Not emulated:** full OData functions (`startswith`, `substringof`, date arithmetic); SAS enforcement; Shared Key signatures are accepted but not verified.

Source: Floci AZ docs, `https://floci.io/floci-az/services/`.

### Key concepts

What the minimal project proves, and where in the code:

| Pattern | Why the pipelines need it | Where |
|---|---|---|
| Ledger upsert + read by key + list a partition | Skip files already `LOADED`; record run outcome | `ledger_put/get/list` |
| Optimistic concurrency | Two consumers on one vehicle must not overwrite each other; the loser re-reads and redoes | `read`, `write_if_unchanged` |
| Insert-only-if-absent | A new vehicle's first write must not clobber a racing writer | `write_if_unchanged(etag=None)` |
| Atomic claim | The first file with a given SHA-256 owns it; the second is a DUPLICATE | `claim` |

### Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Every table call returns 404/`TableNotFound` | Using the blob path as `TableEndpoint` | `.../<account>-table` (see `floci.py`) |
| `ResourceModifiedError` on every write | Re-using an old ETag in a retry loop | Re-`read()` inside the loop to get a fresh ETag |
| A `$filter` with `startswith` fails | Not in Floci's OData subset | Filter on `PartitionKey`/`RowKey` equality and filter the rest in code |

### What was verified

**Ran and passed (build sandbox):**

- 7 tests + `demo.py` against **Azurite Table**: upsert replaces, partitions are separate, stale-ETag write refused (HTTP 412 -> `False`), insert-only-when-absent, first claim wins.

**Not run:**

- Floci AZ itself. The only thing that differs between Azurite and Floci is the connection string (`-table` path), which is documented by Floci but not exercised here.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md) read once. Commands start from this guides folder (`floci-azure-guides/`); the project lives in `project/02-table-storage/`.

### Project structure

```text
project/02-table-storage/
  demo.py
  docker-compose.yml
  floci.py
  requirements.txt
  tablekit.py
  tests/
    conftest.py
    test_tablekit.py
```

### Run it

```bash
cd project/02-table-storage && pip install -r requirements.txt
docker compose up -d
python demo.py
pytest tests -v
```
Azurite works too (its table service is on port 10002): set `STORAGE_CONNECTION_STRING` with `TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;`.

---

## 3. Examples

Every file below is the exact content of `project/02-table-storage/`.

### Example 1 — `docker-compose.yml`

````yaml
services:
  floci-az:
    image: floci/floci-az:latest
    ports:
      - "4577:4577"          # Table lives at  http://localhost:4577/<account>-table
````
### Example 2 — `demo.py`

````python
"""python demo.py -> exercises ledger, ETag race and atomic claim."""
import uuid

import tablekit as tk

t = tk.table(f"demo{uuid.uuid4().hex[:8]}")

tk.ledger_put(t, "claims_20260930.csv.gz", status="TRANSFORMED", etag="abc")
tk.ledger_put(t, "claims_20260930.csv.gz", status="LOADED", etag="abc")           # same key: replaced
print("ledger  ", dict(tk.ledger_get(t, "claims_20260930.csv.gz")), "| rows:", len(tk.ledger_list(t, "file")))

row, etag = tk.read(t, "D-01", "V-0001")
assert row is None and tk.write_if_unchanged(t, {"PartitionKey": "D-01", "RowKey": "V-0001", "status": "OK"}, None)
mine, my_etag = tk.read(t, "D-01", "V-0001")
other, other_etag = tk.read(t, "D-01", "V-0001")                                    # a second consumer reads the same row
assert tk.write_if_unchanged(t, {**dict(other), "status": "EXCURSION"}, other_etag)  # ...and writes first
stale = tk.write_if_unchanged(t, {**dict(mine), "status": "CRITICAL"}, my_etag)      # our write is now stale
print("hot row ", "stale write refused" if not stale else "BUG: stale write accepted", "| status =", tk.read(t, "D-01", "V-0001")[0]["status"])

print("claim   ", tk.claim(t, "hash", "sha256-of-file", name="BAN_20261002_001.csv"), "then", tk.claim(t, "hash", "sha256-of-file", name="copy.csv"))
````
### Example 3 — `floci.py`

````python
"""Connection helper shared by the storage projects.

Floci AZ routes every service by PATH on one port:
    Blob   http://localhost:4577/<account>
    Table  http://localhost:4577/<account>-table        (NOT the blob path)
Set STORAGE_CONNECTION_STRING to point at Azurite or real Azure instead.
"""
import os

ACCOUNT = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="   # public Azurite dev key, not a secret


def conn_str(host="localhost", port=4577):
    return (f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};"
            f"BlobEndpoint=http://{host}:{port}/{ACCOUNT};"
            f"TableEndpoint=http://{host}:{port}/{ACCOUNT}-table;")


def storage_conn_str():
    return os.environ.get("STORAGE_CONNECTION_STRING") or conn_str()
````
### Example 4 — `requirements.txt`

````text
azure-data-tables==12.*
pytest
````
### Example 5 — `tablekit.py`

````python
"""The three Table Storage patterns the pipelines depend on:
  1. LEDGER   - upsert a row per file / per run, read it back by key        (batch ETL: etlledger)
  2. HOT STATE- read row + ETag, write back ONLY if nobody changed it       (streaming: fleetstate)
  3. CLAIM    - atomic create-if-absent, first writer wins                  (DQ gate: dqaudit 'hash' rows)
"""
import json

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError, ResourceNotFoundError
from azure.data.tables import TableServiceClient, UpdateMode

from floci import storage_conn_str


def service(**kw):
    return TableServiceClient.from_connection_string(storage_conn_str(), **kw)


def table(name, svc=None):
    return (svc or service()).create_table_if_not_exists(name)


# 1. ledger ---------------------------------------------------------------------------------------------
def ledger_put(t, key, partition="file", **props):
    t.upsert_entity({"PartitionKey": partition, "RowKey": key, **props})        # upsert: re-running a step overwrites its own row


def ledger_get(t, key, partition="file"):
    try:
        return t.get_entity(partition, key)
    except ResourceNotFoundError:
        return None


def ledger_list(t, partition):
    return list(t.query_entities(f"PartitionKey eq '{partition}'"))


# 2. hot state with optimistic concurrency ----------------------------------------------------------------
def read(t, pk, rk):
    """-> (entity | None, etag | None)"""
    try:
        e = t.get_entity(pk, rk)
        return e, e.metadata["etag"]
    except ResourceNotFoundError:
        return None, None


def write_if_unchanged(t, entity, etag):
    """True if written. False if another writer got there first (HTTP 412 on update, 409 on insert):
    the caller must re-read and redo its work. etag=None means 'insert; the row must not exist yet'."""
    try:
        if etag is None:
            t.create_entity(entity)
        else:
            t.update_entity(entity, mode=UpdateMode.REPLACE, etag=etag, match_condition=MatchConditions.IfNotModified)
        return True
    except (ResourceModifiedError, ResourceExistsError):
        return False


def pack(state: dict) -> str:
    """Whole state in one JSON property; keep a few columns beside it for querying (the 'hot row is a projection' pattern)."""
    return json.dumps(state, separators=(",", ":"))


# 3. atomic claim -----------------------------------------------------------------------------------------
def claim(t, pk, rk, **props):
    """Create-if-absent. True = you own it. False = someone already did (the DQ gate routes the second file DUPLICATE)."""
    try:
        t.create_entity({"PartitionKey": pk, "RowKey": rk, **props})
        return True
    except ResourceExistsError:
        return False
````
### Example 6 — `tests/conftest.py`

````python
import pathlib
import sys
import uuid

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import tablekit as tk  # noqa: E402


@pytest.fixture(scope="session")
def svc():
    """Auto-skips when no table endpoint answers (Floci AZ, or Azurite + STORAGE_CONNECTION_STRING)."""
    s = tk.service(connection_timeout=2, retry_total=0)
    try:
        list(s.list_tables(results_per_page=1))
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"no table endpoint reachable ({type(e).__name__}); run `docker compose up -d` or start Azurite")
    return s


@pytest.fixture
def t(svc):
    name = f"t{uuid.uuid4().hex[:12]}"
    tbl = svc.create_table(name)
    yield tbl
    svc.delete_table(name)
````
### Example 7 — `tests/test_tablekit.py`

````python
import json

import tablekit as tk


def test_pack_is_compact_json():                                   # pure, needs no endpoint
    assert tk.pack({"a": 1, "b": [1, 2]}) == '{"a":1,"b":[1,2]}' and json.loads(tk.pack({"x": "y"})) == {"x": "y"}


def test_ledger_upsert_replaces_not_duplicates(t):
    tk.ledger_put(t, "f1", status="TRANSFORMED")
    tk.ledger_put(t, "f1", status="LOADED")
    assert tk.ledger_get(t, "f1")["status"] == "LOADED" and len(tk.ledger_list(t, "file")) == 1
    assert tk.ledger_get(t, "missing") is None


def test_ledger_partitions_are_separate(t):
    tk.ledger_put(t, "f1", partition="file", status="LOADED")
    tk.ledger_put(t, "b-1", partition="run", status="SUCCEEDED")
    assert [e["RowKey"] for e in tk.ledger_list(t, "run")] == ["b-1"]


def test_insert_only_when_absent(t):
    assert tk.write_if_unchanged(t, {"PartitionKey": "D", "RowKey": "V1", "n": 1}, None) is True
    assert tk.write_if_unchanged(t, {"PartitionKey": "D", "RowKey": "V1", "n": 2}, None) is False   # already exists
    assert tk.read(t, "D", "V1")[0]["n"] == 1


def test_stale_etag_write_is_refused(t):
    tk.write_if_unchanged(t, {"PartitionKey": "D", "RowKey": "V1", "n": 0}, None)
    (a, ea), (b, eb) = tk.read(t, "D", "V1"), tk.read(t, "D", "V1")
    assert tk.write_if_unchanged(t, {**dict(a), "n": 1}, ea) is True          # first writer wins
    assert tk.write_if_unchanged(t, {**dict(b), "n": 2}, eb) is False         # second is refused: re-read and redo
    assert tk.read(t, "D", "V1")[0]["n"] == 1


def test_fresh_etag_write_succeeds_again(t):
    tk.write_if_unchanged(t, {"PartitionKey": "D", "RowKey": "V1", "n": 0}, None)
    for n in (1, 2, 3):                                                       # the retry loop: re-read, write, repeat
        row, etag = tk.read(t, "D", "V1")
        assert tk.write_if_unchanged(t, {**dict(row), "n": n}, etag)
    assert tk.read(t, "D", "V1")[0]["n"] == 3


def test_claim_first_writer_owns_it(t):
    assert tk.claim(t, "hash", "sha-1", name="a.csv") is True
    assert tk.claim(t, "hash", "sha-1", name="b.csv") is False                # the second copy is a DUPLICATE
    assert tk.read(t, "hash", "sha-1")[0]["name"] == "a.csv"
````
### Example 8 — On real Azure (Terraform and auth)

- Terraform: the same `azurerm_storage_account` as Blob (Table needs no extra resource).
- Auth: *Storage Table Data Contributor* via managed identity.
- Outgrowing it: **Cosmos DB for Table** accepts the same `azure-data-tables` calls.

---

## Reference

Project files:

```
demo.py
docker-compose.yml
floci.py
requirements.txt
tablekit.py
tests/conftest.py
tests/test_tablekit.py
```

AWS counterpart: [13 DynamoDB](../floci-aws-guides/13-dynamodb.md).
