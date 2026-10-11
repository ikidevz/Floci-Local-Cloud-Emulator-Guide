# 01 — Blob Storage

> [Index](README.md) · [← 00 — Setup & Shared Foundation](00-setup-floci.md) · [02 — Table Storage →](02-table-storage.md)

Object storage used as the *zones* of every pipeline: where files land, where intermediate data sits, where rejects and evidence go.

---

## 1. Documentation

### Overview

- **Azure Blob Storage** — Object storage used as the *zones* of every pipeline: where files land, where intermediate data sits, where rejects and evidence go.

**Where the three pipelines use it**

| Pipeline | Role | Pattern that matters |
|---|---|---|
| 1 Batch ETL | `landing/` (feeds), `stage/` (conformed data), `rejects/` (reason-coded bad rows) | partitioned prefixes `feed/dt=DATE/`, ETag read by the file ledger, gzip CSV |
| 2 Streaming | `archive/` (enriched events), `rejects/` (dead letter), `registry/` (reference JSON) | deterministic blob names so a replayed batch overwrites itself |
| 3 DQ gate | `inbox/`, `accepted/`, `quarantine/`, `rejects/`, `reports/`, `registry/` | quarantine keeps the **original bytes**; names are deterministic |

### At a glance

| Item | Value |
|------|-------|
| Python packages | `azure-storage-blob==12.*`, `pytest` |
| Floci AZ ports published | `4577` |
| Docker socket | not required |
| Project folder | `project/01-blob-storage/` |
| Tests | 6 |
| Verified against | **Azurite** (real Blob REST API) |

### How it behaves on Floci

- **Endpoint:** `http://localhost:4577/<account>` (path-style; any account name; the public Azurite dev key is accepted, signatures are not verified).
- **Emulated:** create/delete containers, upload/download/delete/list blobs, prefix listing, ETags.
- **You must create containers yourself** - Floci creates none. `ensure_zones()` does it idempotently.

Source: Floci AZ docs, `https://floci.io/floci-az/services/`.

### Key concepts

What the minimal project proves, and where in the code:

| Pattern | Why the pipelines need it | Where |
|---|---|---|
| Zones exist, created idempotently | Floci starts empty; every pipeline assumes its containers | `blobzones.ensure_zones` |
| Partitioned prefixes + listing | The batch plan asks 'what landed under `dt=...`?' | `partition`, `list_prefix` |
| Same name = replace, ETag changes | Idempotent replays; the ledger detects a re-delivered file by ETag | `put(overwrite=True)`, tests |
| gzip CSV round-trip | All feeds and stage data are `csv.gz` | `to_gz_csv`, `from_gz_csv` |
| Byte-exact promotion | Quarantine must keep the original bytes, even binary garbage | `promote` |

### Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| `ResourceNotFound` on upload | The container does not exist | `ensure_zones()` first |
| Tests all skipped | No endpoint answering | `docker compose up -d`, or Azurite + `STORAGE_CONNECTION_STRING` |
| A job duplicates output on replay | A name containing a timestamp/UUID | Derive names from the *input* (batch id, hash of ids) |

### What was verified

**Ran and passed (build sandbox):**

- 6 tests + `demo.py` against **Azurite** (the real Azure Blob REST API): zones idempotent, prefix isolation, overwrite keeps one file but changes the ETag, binary-exact promotion.

**Not run:**

- Floci AZ itself (no Docker in the build sandbox). Blob is the most mature Floci service and uses the same SDK calls, but this specific run is unverified.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md) read once. Commands start from this guides folder (`floci-azure-guides/`); the project lives in `project/01-blob-storage/`.

### Project structure

```text
project/01-blob-storage/
  blobzones.py
  demo.py
  docker-compose.yml
  floci.py
  requirements.txt
  tests/
    conftest.py
    test_blobzones.py
```

### Run it

```bash
cd project/01-blob-storage && pip install -r requirements.txt
docker compose up -d
python demo.py              # lands, lists, replays (still one file), rejects, promotes
pytest tests -v             # auto-SKIPS if nothing answers; never fails for a missing emulator
```
No Docker? Use Azurite and point the same code at it:
```bash
npm i -g azurite && azurite --silent --skipApiVersionCheck &
export STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
```

---

## 3. Examples

Every file below is the exact content of `project/01-blob-storage/`.

### Example 1 — `docker-compose.yml`

````yaml
services:
  floci-az:
    image: floci/floci-az:latest
    ports:
      - "4577:4577"          # Blob lives at  http://localhost:4577/<account>
````
### Example 2 — `blobzones.py`

````python
"""The Blob patterns the three pipelines rely on, and nothing else:
zones (containers), partitioned prefixes (feed/dt=DATE/), deterministic names, gzip CSV, byte-exact promotion."""
import csv
import gzip
import io

from azure.storage.blob import BlobServiceClient

from floci import storage_conn_str

# batch ETL: landing/stage/rejects | streaming: archive/rejects/registry | DQ gate: inbox/accepted/quarantine/rejects/reports/registry
ZONES = ("landing", "stage", "rejects", "archive", "inbox", "accepted", "quarantine", "reports", "registry")


def service(**kw):
    return BlobServiceClient.from_connection_string(storage_conn_str(), **kw)


def ensure_zones(svc, zones=ZONES):
    """Idempotent: containers are created only if missing (Floci creates none for you)."""
    for z in zones:
        c = svc.get_container_client(z)
        if not c.exists():
            c.create_container()


def put(svc, container, name, data: bytes):
    """overwrite=True + a DETERMINISTIC name = a replayed job replaces its own output instead of duplicating it."""
    svc.get_blob_client(container, name).upload_blob(data, overwrite=True)


def get(svc, container, name) -> bytes:
    return svc.get_blob_client(container, name).download_blob().readall()


def partition(feed, day):
    return f"{feed}/dt={day}/"


def list_prefix(svc, container, prefix):
    """-> [(name, etag)] under a prefix. The ETag is what a file ledger compares to spot a re-delivered file."""
    return [(b.name, b.etag.strip('"')) for b in svc.get_container_client(container).list_blobs(name_starts_with=prefix)]


def to_gz_csv(header, rows) -> bytes:
    buf = io.StringIO(newline="")
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return gzip.compress(buf.getvalue().encode())


def from_gz_csv(raw: bytes):
    r = csv.reader(io.StringIO(gzip.decompress(raw).decode()))
    header = next(r, [])
    return header, list(r)


def promote(svc, src, name, dst, dst_name=None):
    """Copy bytes untouched from one zone to another (quarantine keeps the ORIGINAL bytes as evidence)."""
    put(svc, dst, dst_name or name, get(svc, src, name))
````
### Example 3 — `demo.py`

````python
"""python demo.py  -> lands a gzip CSV, lists the partition, replays it (idempotent), rejects a row."""
import blobzones as bz

svc = bz.service()
bz.ensure_zones(svc)

day, name = "2026-10-01", "claims_20260930.csv.gz"
key = bz.partition("claims", day) + name
payload = bz.to_gz_csv(["claim_id", "billed_total"], [["C-1", "10.00"], ["C-2", "20.00"]])

bz.put(svc, "landing", key, payload)
print("landed  ", bz.list_prefix(svc, "landing", bz.partition("claims", day)))

bz.put(svc, "landing", key, payload)                       # same name again: replaced, not duplicated
files = bz.list_prefix(svc, "landing", bz.partition("claims", day))
assert len(files) == 1, files
print("replayed", files, "(still one file)")

header, rows = bz.from_gz_csv(bz.get(svc, "landing", key))
print("read    ", header, rows)

bz.put(svc, "rejects", "batch=demo/claims_2026-10-01.csv.gz", bz.to_gz_csv(["reason", "raw"], [["BAD_NPI", "C-2,..."]]))
bz.promote(svc, "landing", key, "quarantine", name)
print("OK")
````
### Example 4 — `floci.py`

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
### Example 5 — `requirements.txt`

````text
azure-storage-blob==12.*
pytest
````
### Example 6 — `tests/conftest.py`

````python
import pathlib
import sys
import uuid

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import blobzones as bz  # noqa: E402


@pytest.fixture(scope="session")
def svc():
    """Auto-skips when no storage endpoint answers (start Floci AZ, or Azurite + STORAGE_CONNECTION_STRING)."""
    s = bz.service(connection_timeout=2, retry_total=0)
    try:
        list(s.list_containers(results_per_page=1))
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"no blob endpoint reachable ({type(e).__name__}); run `docker compose up -d` or start Azurite")
    return s


@pytest.fixture
def box(svc):
    name = f"t-{uuid.uuid4().hex[:10]}"
    svc.create_container(name)
    yield name
    svc.delete_container(name)
````
### Example 7 — `tests/test_blobzones.py`

````python
import blobzones as bz


def test_gz_csv_roundtrip_is_pure():                          # needs no endpoint
    header, rows = bz.from_gz_csv(bz.to_gz_csv(["a", "b"], [["1", "x,y"], ["2", 'q"r']]))
    assert header == ["a", "b"] and rows == [["1", "x,y"], ["2", 'q"r']]


def test_partition_prefix():
    assert bz.partition("claims", "2026-10-01") == "claims/dt=2026-10-01/"


def test_ensure_zones_is_idempotent(svc):
    bz.ensure_zones(svc)
    bz.ensure_zones(svc)                                      # second call must not raise
    assert {c.name for c in svc.list_containers()} >= set(bz.ZONES)


def test_prefix_listing_sees_only_its_partition(svc, box):
    bz.put(svc, box, "claims/dt=2026-10-01/a.csv.gz", b"1")
    bz.put(svc, box, "claims/dt=2026-10-02/b.csv.gz", b"2")
    assert [n for n, _ in bz.list_prefix(svc, box, bz.partition("claims", "2026-10-01"))] == ["claims/dt=2026-10-01/a.csv.gz"]


def test_same_name_replaces_and_etag_changes(svc, box):
    bz.put(svc, box, "x/f.csv.gz", b"v1")
    (_, e1), = bz.list_prefix(svc, box, "x/")
    bz.put(svc, box, "x/f.csv.gz", b"v2")
    listing = bz.list_prefix(svc, box, "x/")
    assert len(listing) == 1 and listing[0][1] != e1          # a changed ETag is how a ledger notices a re-delivered file
    assert bz.get(svc, box, "x/f.csv.gz") == b"v2"


def test_promote_keeps_bytes_exact(svc, box):
    raw = bytes(range(256)) * 4                               # binary garbage, as the DQ gate must keep it
    bz.put(svc, box, "inbox/bad.csv", raw)
    bz.promote(svc, box, "inbox/bad.csv", box, "quarantine/bad.csv")
    assert bz.get(svc, box, "quarantine/bad.csv") == raw
````
### Example 8 — On real Azure (Terraform and auth)

- Terraform: `azurerm_storage_account` (+ `is_hns_enabled` for ADLS Gen2) and `azurerm_storage_container` (the DQ gate makes `inbox/` the only partner-writable zone).
- Auth: drop the connection string; `DefaultAzureCredential` + the *Storage Blob Data Contributor* role.
- Event Grid blob events exist only on real Azure (see [guide 08](08-event-grid.md)).

---

## Reference

Project files:

```
blobzones.py
demo.py
docker-compose.yml
floci.py
requirements.txt
tests/conftest.py
tests/test_blobzones.py
```

AWS counterpart: [01 S3](../floci-aws-guides/01-s3.md).
Related: [08 Event Grid](08-event-grid.md).
