# 00 — Setup & Shared Foundation

> [Index](README.md) · [01 — Blob Storage →](01-blob-storage.md)

Everything the other guides rely on: running Floci AZ, how its single port routes each service, the shared connection helper, and the conventions every `project/NN-service/` follows.

---

## 1. Documentation

### Overview

- [Floci AZ](https://floci.io/floci-az/services/) is the Azure side of Floci: one container, one port (`4577`), every service routed by **URL path**.
- Credentials are not verified. The public Azurite dev account (`devstoreaccount1` + its well-known key) is accepted. It is not a secret.
- Each service guide has its **own** `project/NN-service/` with its own `docker-compose.yml` enabling only what that service needs, so any guide runs on its own.

### At a glance

| Item | Value |
|------|-------|
| Image | `floci/floci-az:latest` |
| Endpoint | `http://localhost:4577/<account>` (blob); other services add a suffix, below |
| Blob | `/<account>` |
| Table | `/<account>-table` (**not** the blob path) |
| Functions | `/<account>-functions` |
| Key Vault | `/<account>-keyvault` |
| Event Hubs | management `/<acct>-eventhub`; AMQP on `5672` |
| Service Bus | management `/<acct>-servicebus`; AMQP on `5673`, AMQPS on `5674` |
| Event Grid | publish `/<topic>-eventgrid/api/events` (HTTP, no sidecar) |
| Docker socket | required for Functions, Azure SQL, Event Hubs and Service Bus (they start real containers) |
| Credentials | `devstoreaccount1` + the public Azurite key |

### Why the compose settings matter

- **Docker socket** (`/var/run/docker.sock`): Functions runs the real Azure Functions runtime as a container; SQL starts a real SQL Server container; Event Hubs and Service Bus start an Artemis broker sidecar.
- **Azure SQL**: `FLOCI_AZ_SERVICES_SQL_DATA_PLANE_PROVIDER=managed` (the default `none` is ARM metadata only, no TDS endpoint) and `FLOCI_AZ_SERVICES_SQL_ACCEPT_EULA=Y`. The SQL Server container binds a random host port; read it from the `.../connect` endpoint. x86-64 only.
- **Event Hubs**: hubs are pre-configured, not created at runtime (`FLOCI_AZ_SERVICES_EVENT_HUB_ENTITIES=telemetry:4`, as `name:partitions`).
- **Service Bus**: `FLOCI_AZ_SERVICES_SERVICE_BUS_MOCKED=false` (the default `true` answers management calls but cannot send or receive) and `START_ON_BOOT=true` (otherwise nothing listens on `5673` until an entity exists). Topology comes from a mounted `Config.json`.
- **Event Grid**: Floci calls your webhook **from inside the container**, so use `host.docker.internal:<port>`, not `localhost`. The compose file adds `extra_hosts: host.docker.internal:host-gateway`. On native Linux Docker with UFW, if invocations time out: `sudo ufw allow in on docker0`.

### Conventions

- Run **one project at a time**: they all publish port `4577`. Or merge the `environment:` blocks into one compose file.
- Set `STORAGE_CONNECTION_STRING` to point the storage projects at Azurite or real Azure instead of Floci.
- `pytest tests -v` can be run before and after `docker compose up -d`. Tests that need Floci skip themselves when nothing is listening, so a missing emulator never shows up as a failure.
- No Terraform project here. Floci's own Terraform suite covers resource group, storage and Key Vault only, so every project provisions through the emulator's own APIs. Each guide's last example names the `azurerm` resources for real Azure.

### What was verified

See the table in the [README](README.md#what-was-verified-and-what-was-not): Floci AZ itself, Docker, SQL Server and the AMQP brokers were **not available** where this was built, so verification used real SDKs and stand-ins.

---

## 2. Basic Usage

### Step 1: Pick a service and start Floci AZ

```bash
cd project/01-blob-storage
docker compose up -d
```

### Step 2: Install the Python dependencies

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # 03-08 use requirements-dev.txt for tests
```

### Step 3: Verify

```bash
pytest tests -v                          # Floci-dependent tests skip if nothing answers
python demo.py                           # most projects ship one (03 uses scripts/deploy.py and make)
```

### Step 4: Switch service

```bash
docker compose down                      # frees port 4577 for the next project
cd ../02-table-storage
```

---

## 3. Examples

### Example 1 — `floci.py`, the connection helper

Shared by the storage projects (01 and 02). It encodes the path-routing rule that Blob and Table live at different paths.

```python
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
```

### Example 2 — No Docker: Azurite for Blob and Table

```bash
npm i -g azurite && azurite --silent --skipApiVersionCheck &
export STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
```

### Example 3 — Which compose settings each project needs

| Guide | Ports | Docker socket | Extra settings |
|-------|-------|:-------------:|----------------|
| [01 Blob Storage](01-blob-storage.md) | `4577` | no | — |
| [02 Table Storage](02-table-storage.md) | `4577` | no | — |
| [03 Azure Functions](03-azure-functions.md) | `4577` | yes | — |
| [04 Azure SQL Database](04-azure-sql-database.md) | `4577` | yes | SQL provider `managed`, EULA accepted |
| [05 Key Vault](05-key-vault.md) | `4577` | no | — |
| [06 Event Hubs](06-event-hubs.md) | `4577`, `5672` | yes | hub `telemetry:4` |
| [07 Service Bus](07-service-bus.md) | `4577`, `5673`, `5674` | yes | `MOCKED=false`, `START_ON_BOOT=true`, `Config.json` |
| [08 Event Grid](08-event-grid.md) | `4577` | no | `host.docker.internal` for webhooks |

---

## Reference

Project files: see each guide's Reference section. Source for Floci AZ behavior: `https://floci.io/floci-az/services/`.
