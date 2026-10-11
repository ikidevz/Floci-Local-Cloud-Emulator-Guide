# Azure on Floci: Python SDK Guides (uniform layout)

One guide per service. Each guide has: Floci-specific behavior (from Floci AZ's own docs), file structure, **code included straight from the runnable `project/NN-service/` folder**, tests, real-Azure notes, and gotchas.

[Floci AZ](https://floci.io/floci-az/services/) is the Azure side of Floci: a local emulator on `http://localhost:4577`. The services here are the ones the three IkiData pipelines (batch ETL, streaming, event-driven DQ gate) actually exercise.

## Layout

```
floci-azure-guides/
├── README.md                      you are here
├── 00-setup-floci.md … 08-event-grid.md
└── project/                       the runnable code the guides are built from
    ├── 01-blob-storage/ … 08-event-grid/
    │   └── docker-compose.yml  requirements*.txt  <service module>.py  demo.py  tests/
    └── one standalone project per service (each enables only what it needs)
```

## Guide index

| # | Guide | Used by pipeline |
|---|-------|------------------|
| 00 | [Setup & foundation](00-setup-floci.md) | one port, path routing, connection helper |
| 01 | [Blob Storage](01-blob-storage.md) | Batch ETL, Streaming, DQ gate |
| 02 | [Table Storage](02-table-storage.md) | Batch ETL, Streaming, DQ gate |
| 03 | [Azure Functions](03-azure-functions.md) | Batch ETL, Streaming, DQ gate |
| 04 | [Azure SQL Database](04-azure-sql-database.md) | Batch ETL |
| 05 | [Key Vault](05-key-vault.md) | Batch ETL |
| 06 | [Event Hubs](06-event-hubs.md) | Streaming |
| 07 | [Service Bus](07-service-bus.md) | Streaming, DQ gate |
| 08 | [Event Grid](08-event-grid.md) | DQ gate |

Suggested order: `00 → 01 → 02 → 05 → 08 → 03 → 07 → 06 → 04`. `01` and `02` first (simplest, and they prove the `-table` fix), `04` last (large SQL Server image, x86-64 host).

### Present in the pipelines but deliberately without a guide

| Item | Why |
|---|---|
| Terraform `azurerm` / Resource Group | Tooling, not a runtime service. Floci's own Terraform suite covers resource group, storage and Key Vault only; Functions, SQL, Event Hubs, Service Bus and Event Grid are not in that list. Every project here provisions through the emulator's own APIs instead, and each guide's "On real Azure" section names the Terraform resources. |
| Managed identity / RBAC | Created only in `prod_function.tf` (real Azure). Floci emulates a managed-identity endpoint, but none of the pipeline code exercises it. |
| Azure Monitor / Application Insights alert + action group | Real-Azure-only resources in `prod_function.tf`; no code path touches them. |

If you want any of these three as a guide too, say so - they are small additions.

## Quick start

```bash
cd project/01-blob-storage
docker compose up -d                  # Floci AZ on :4577
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
pytest tests -v                       # Floci-dependent tests skip themselves if nothing answers
python demo.py                        # lands, lists, replays, rejects, promotes
docker compose down                   # projects share port 4577: run one at a time
```

## What was verified, and what was not

Floci AZ itself, Docker, SQL Server and the AMQP brokers were **not available** where this was built. Verification used real SDKs and
stand-ins; the "Not verified" column is where your first `docker compose up` will tell you something.

| # | Tests | Verified against | Not verified |
|---|---|---|---|
| 01 | 6 | **Azurite** (real Blob REST API) | Floci AZ |
| 02 | 7 | **Azurite** (real Table REST API: ETags, atomic create) | Floci AZ (only the `-table` connection string differs) |
| 03 | 4 | unit tests; `make package` run, ZIP layout checked | deployment to and invocation on Floci |
| 04 | 9 | every statement parsed as **T-SQL**; the same load SQL executed on **SQLite** (replay, replace, rollback, reconcile); mutation-checked | SQL Server itself; `provision.py`, `connect.py`, `demo.py` only compiled |
| 05 | 4 (+2 Floci-only, skipped) | a **fake vault** that performs the real 401 challenge handshake | Floci Key Vault |
| 06 | 8 | unit tests with fakes; SDK source inspected | any broker |
| 07 | 15 | `Config.json` limits; every subscription filter **executed** on sample messages; mutation-checked | any broker; the `Rules` schema in `Config.json` was written from memory of the official emulator format |
| 08 | 8 | unit tests + a **live local HTTP server** | every call to Floci's Event Grid |

**61 tests pass, 2 skip** (the skips need a running Floci AZ). Two real defects were found and fixed during the build: a vacuous
`or True` assertion in my own SQL test, and a URL-rewriting bug that breaks the Key Vault client after a 401 challenge.

## Where the uploaded blueprints and Floci's documentation disagree

Checked against the Floci AZ service pages (`floci.io/floci-az/services/...`). These affect whether the uploaded code runs as written.

1. **Table endpoint.** All three uploads set `TableEndpoint=http://localhost:4577/devstoreaccount1` (the blob path). Floci routes
   Table Storage at **`/<account>-table`**. As written, table calls would go to the wrong service. Fixed in `floci.py` here.
2. **Event Hub name.** Pipeline 2's compose file never configures hubs, but Floci creates hubs only from config
   (`FLOCI_AZ_SERVICES_EVENT_HUB_ENTITIES`, default `eh1:4`), so `telemetry` would not exist. Set to `telemetry:4` here.
3. **Checkpointing.** The streaming bridge calls `update_checkpoint()` "only on 200". In `azure-eventhub`, that call without a
   checkpoint store is a no-op that logs a warning, so nothing was being protected. Guide 06 makes the store explicit.
4. **Service Bus.** The uploads take the connection string "from the namespace's authorization rule". Floci documents a fixed
   development string, declarative topology via `Config.json`, and **`START_ON_BOOT=true`** (without it nothing listens on 5673 until
   an entity exists). The default `MOCKED=true` is already called out in the uploads.
5. **Key Vault SDK.** Floci's documented Python snippet rewrites https to http permanently, which breaks the retry after a
   401 challenge. Guide 05 restores the URL after each send.
6. **Azure SQL image.** Floci's GitHub README says `azure-sql-edge`; its SQL page says SQL Server 2025 with the EULA flag. The
   uploads and this project follow the SQL page.
