# 04 — Azure SQL Database

> [Index](README.md) · [← 03 — Azure Functions](03-azure-functions.md) · [05 — Key Vault →](05-key-vault.md)

The warehouse of the batch pipeline: a conformed star schema loaded in a single transaction, then reconciled against control totals.

---

## 1. Documentation

### Overview

- **Azure SQL Database** — The warehouse of the batch pipeline: a conformed star schema loaded in a single transaction, then reconciled against control totals.

**Where the three pipelines use it**

| Pipeline | Role | Pattern that matters |
|---|---|---|
| 1 Batch ETL | `dw.*` star schema (7 dimensions, 3 facts) + `stg.*` load staging | stage -> delete by key -> insert in **one transaction**; reconcile counts and money |

### At a glance

| Item | Value |
|------|-------|
| Python packages | `pyodbc` |
| Dev / test packages | `pytest`, `sqlglot` |
| Floci AZ ports published | `4577` |
| Docker socket | required |
| Floci settings in compose | `FLOCI_AZ_SERVICES_SQL_DATA_PLANE_PROVIDER`, `FLOCI_AZ_SERVICES_SQL_ACCEPT_EULA` |
| Project folder | `project/04-azure-sql-database/` |
| Tests | 9 |
| Verified against | every statement parsed as **T-SQL**; the same load SQL executed on **SQLite** (replay, replace, rollback, reconcile); mutation-checked |

### How it behaves on Floci

- **Management plane:** ARM paths `Microsoft.Sql/servers/...` (create server -> `202`, create database) and a Floci convenience path `/<account>-sql/servers/<name>/connect` that returns the host and **dynamic port**.
- **Data plane:** set `FLOCI_AZ_SERVICES_SQL_DATA_PLANE_PROVIDER=managed` to get a **real SQL Server container** per logical server (image `mcr.microsoft.com/mssql/server:2025-latest`). The default `none` gives ARM metadata only - no TDS endpoint.
- **Requirements:** Docker socket, `FLOCI_AZ_SERVICES_SQL_ACCEPT_EULA=Y`, an **x86-64** host (Microsoft supports the image only there). Server creation is asynchronous; the first run pulls the image.
- **Not enforced:** firewall rules are metadata only.

Source: Floci AZ docs, `https://floci.io/floci-az/services/`.

### Key concepts

What the minimal project proves, and where in the code:

| Pattern | Why the pipelines need it | Where |
|---|---|---|
| Provision server + database, find the dynamic port | Never hard-code 1433 | `provision.py` |
| Schema defined once as data; idempotent T-SQL DDL | `up` can run any number of times; DDL and load SQL cannot drift | `schema.py` |
| Stage -> delete-by-key -> insert, atomic | A re-delivered row *replaces*; a failure rolls back dimensions and facts together | `load.load_batch` |
| Stay under SQL Server's 2,100-parameter limit | Bulk inserts as multi-row statements | `load.insert_rows` |
| Reconcile control totals | A mismatch is a *data* failure and must stop the run | `load.reconcile` |

### Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| `EulaNotAccepted` on server create | EULA variable missing | `FLOCI_AZ_SERVICES_SQL_ACCEPT_EULA=Y` |
| `/connect` returns 409 `DataPlaneNotEnabled` | Provider is the default `none` | `..._DATA_PLANE_PROVIDER=managed` |
| Provisioning seems stuck | First run pulls a large image; creation is async (202) | Wait; `provision.py` polls up to 15 minutes |
| Container will not start on Apple Silicon / ARM | Microsoft supports the image on x86-64 only | Use an x86-64 host or set `FLOCI_AZ_SERVICES_SQL_IMAGE` |
| TLS error connecting | Self-signed certificate | `TrustServerCertificate=yes` locally only |
| "Too many parameters" | More than 2,100 in one statement | `insert_rows` sizes statements to `2000 // columns` rows |

### What was verified

**Ran and passed (build sandbox):**

- 9 tests: **every emitted statement parses as T-SQL** (`sqlglot`); the idempotent `IF OBJECT_ID...` guards; parameter chunking; ODBC string quoting; and the **same load SQL executed on SQLite**: load + reconcile, reload never duplicates, a re-delivered row replaces its old version, a mid-load failure rolls back dimensions *and* facts, reconcile fails on a wrong count or wrong money.
- Mutation-checked: removing the delete-by-key makes two tests fail.

**Not run:**

- **SQL Server itself** - no SQL Server or Docker in the build sandbox. The statements are plain and parse as T-SQL, but they have never executed on SQL Server. `provision.py`, `connect.py` and `demo.py` were only compiled.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md) read once. Commands start from this guides folder (`floci-azure-guides/`); the project lives in `project/04-azure-sql-database/`.

### Project structure

```text
project/04-azure-sql-database/
  connect.py
  demo.py
  docker-compose.yml
  load.py
  provision.py
  requirements-dev.txt
  requirements.txt
  schema.py
  tests/
    conftest.py
    test_sql.py
```

### Run it

```bash
cd project/04-azure-sql-database && pip install -r requirements-dev.txt
pytest tests -v                                   # no SQL Server needed (see Verification)
# against Floci AZ (x86-64 + Docker):
docker compose up -d
eval "$(python provision.py)"                     # waits for the container; prints `export SQL_HOST=... SQL_PORT=...`
python demo.py                                    # DDL, load twice, reconcile
```
`demo.py` needs the **ODBC Driver 18 for SQL Server** installed on your machine (Microsoft's `msodbcsql18` package) - `pyodbc` is only the Python binding.

---

## 3. Examples

Every file below is the exact content of `project/04-azure-sql-database/`.

### Example 1 — `docker-compose.yml`

````yaml
services:
  floci-az:
    image: floci/floci-az:latest
    ports:
      - "4577:4577"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock                 # REQUIRED: a real SQL Server container is started per logical server
    environment:
      FLOCI_AZ_SERVICES_SQL_DATA_PLANE_PROVIDER: managed          # default is 'none' = ARM metadata only, no TDS endpoint
      FLOCI_AZ_SERVICES_SQL_ACCEPT_EULA: "Y"                      # you must accept the Microsoft SQL Server EULA
      # The SQL Server container binds a RANDOM host port. Do not publish it here; read it from  .../connect
      # Microsoft supports the SQL Server Linux image on x86-64 only (ARM: set FLOCI_AZ_SERVICES_SQL_IMAGE to something that runs there).
````
### Example 2 — `connect.py`

````python
"""Warehouse connection from environment. pyodbc + 'ODBC Driver 18 for SQL Server' is the client Floci AZ documents.
The emulated server uses a self-signed certificate, hence TrustServerCertificate=yes (real Azure SQL: use a valid cert)."""
import os


def _esc(v):
    return "{" + str(v).replace("}", "}}") + "}"                    # ODBC brace-quoting: safe for ; = ! etc. in passwords


def odbc_string(host, port, db, user, password):
    return (f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host},{port};DATABASE={db};UID={_esc(user)};PWD={_esc(password)};"
            "Encrypt=yes;TrustServerCertificate=yes;")


def connect_from_env():
    import pyodbc                                                   # lazy: tests and DDL generation need no driver
    e = os.environ
    return pyodbc.connect(odbc_string(e.get("SQL_HOST", "localhost"), e["SQL_PORT"], e.get("SQL_DB", "demo"), e.get("SQL_USER", "sa"), e["SQL_PASSWORD"]),
                          autocommit=False, timeout=15)
````
### Example 3 — `demo.py`

````python
"""python demo.py   (after `python provision.py` and exporting its output)
Creates the schema, loads a batch twice, proves the second load changed nothing, then reconciles."""
from decimal import Decimal

import connect
import load as ld
import schema

conn = connect.connect_from_env()
cur = conn.cursor()
for stmt in schema.tsql_ddl():                                      # idempotent
    cur.execute(stmt)
conn.commit()

dims = {"dim_plan": (ld.columns("dim_plan"), [[1, "PL01", "HMO", Decimal("0.30")], [2, "PL02", "PPO", Decimal("0.20")]])}
facts = {"fact_claim": (ld.columns("fact_claim"), [["C-1", 1, Decimal("100.00"), Decimal("70.00"), "claims_20260930.csv.gz", "b-1"],
                                                    ["C-2", 2, Decimal("50.00"), Decimal("40.00"), "claims_20260930.csv.gz", "b-1"]])}
for attempt in (1, 2):
    ld.load_batch(conn, dims, facts)
    print(f"load #{attempt}:", ld.reconcile(conn, "b-1", {"fact_claim": 2}, "110.00"))
cur.execute("SELECT COUNT(*) FROM dw.fact_claim")
assert cur.fetchone()[0] == 2, "re-load duplicated rows"
print("OK: re-load replaced, did not duplicate")
````
### Example 4 — `load.py`

````python
"""One-transaction warehouse load. Deliberately PLAIN SQL (DELETE ... WHERE IN, INSERT ... SELECT): valid T-SQL, and it runs
unchanged on SQLite, so the same statements are executed in the tests. Placeholder is '?' for both pyodbc and sqlite3."""
from decimal import Decimal

from schema import DIMS, FACTS, SPEC


class ReconciliationError(Exception):
    pass


def _plain(v):
    return str(v) if isinstance(v, Decimal) else v                  # Decimal -> str: drivers disagree about Decimal binding


def insert_rows(cur, table, cols, rows, max_params=2000):
    """Multi-row parameterised INSERT. SQL Server allows 2100 parameters per statement -> rows per statement = 2000 // columns."""
    per = max(1, max_params // len(cols))
    one = "(" + ",".join(["?"] * len(cols)) + ")"
    for i in range(0, len(rows), per):
        chunk = rows[i:i + per]
        cur.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES " + ",".join([one] * len(chunk)), [_plain(v) for r in chunk for v in r])


def load_batch(conn, dims, facts):
    """dims/facts: {table: (columns, rows)}. ATOMIC: a failure anywhere rolls back dimensions AND facts together.
    Dimensions: full refresh (surrogate keys are deterministic). Facts: stage -> delete by key -> insert, so a
    re-delivered row REPLACES its old version and a re-run never duplicates."""
    cur = conn.cursor()
    try:
        for t, (cols, rows) in dims.items():
            cur.execute(f"DELETE FROM dw.{t}")
            insert_rows(cur, f"dw.{t}", cols, rows)
        for t, (cols, rows) in facts.items():
            key, names = FACTS[t], ", ".join(cols)
            cur.execute(f"DELETE FROM stg.{t}")
            insert_rows(cur, f"stg.{t}", cols, rows)
            cur.execute(f"DELETE FROM dw.{t} WHERE {key} IN (SELECT {key} FROM stg.{t})")
            cur.execute(f"INSERT INTO dw.{t} ({names}) SELECT {names} FROM stg.{t}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def reconcile(conn, batch_id, expected_rows: dict, expected_plan_paid: str):
    """Control totals: what the warehouse holds for this batch must equal what the transform produced. A mismatch is a DATA failure."""
    cur = conn.cursor()
    got = {}
    for t in FACTS:
        cur.execute(f"SELECT COUNT(*) FROM dw.{t} WHERE batch_id = ?", [batch_id])
        got[t] = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(plan_paid_total), 0) FROM dw.fact_claim WHERE batch_id = ?", [batch_id])
    paid = Decimal(str(cur.fetchone()[0])).quantize(Decimal("0.01"))
    if got != expected_rows or paid != Decimal(expected_plan_paid).quantize(Decimal("0.01")):
        raise ReconciliationError(f"warehouse {got} / plan paid {paid} != transform {expected_rows} / {expected_plan_paid}")
    return {"loaded": got, "plan_paid": str(paid)}


def columns(table):
    return [c for c, _ in SPEC[table]]
````
### Example 5 — `provision.py`

````python
"""python provision.py  ->  creates the logical server + database in Floci AZ and prints the connection settings.

Floci AZ (data-plane provider 'managed'):
  PUT  /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Sql/servers/<name>   -> 202, a real SQL Server container starts
  GET  /<acct>-sql/servers/<name>/connect                                                 -> 200 {host, port, ...} once it is ready
The port is DYNAMIC. Never hard-code 1433. The first run pulls the SQL Server image, so be patient.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE, ACCT = os.environ.get("FLOCI_ENDPOINT", "http://localhost:4577"), "devstoreaccount1"
SERVER, DB = os.environ.get("SQL_SERVER", "demo-sql"), os.environ.get("SQL_DB", "demo")
USER, PASSWORD = "sa", os.environ.get("SQL_PASSWORD", "Str0ng!Passw0rd")          # must satisfy SQL Server's password policy
ARM = f"{BASE}/subscriptions/dev/resourceGroups/rg/providers/Microsoft.Sql/servers"


def http(method, url, body=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {}


def wait_for_port(timeout_s=900):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        code, body = http("GET", f"{BASE}/{ACCT}-sql/servers/{SERVER}/connect")
        if code == 200 and "port" in body:
            return body["port"]
        time.sleep(5)
    sys.exit("SQL Server container did not become ready (is Docker reachable? FLOCI_AZ_SERVICES_SQL_ACCEPT_EULA=Y set?)")


def provision():
    code, body = http("PUT", f"{ARM}/{SERVER}?api-version=2021-11-01",
                      {"location": "eastus", "properties": {"administratorLogin": USER, "administratorLoginPassword": PASSWORD}})
    if code not in (200, 201, 202):
        sys.exit(f"server create failed: {code} {body}")                          # e.g. EulaNotAccepted
    port = wait_for_port()
    code, body = http("PUT", f"{ARM}/{SERVER}/databases/{DB}?api-version=2021-11-01", {"location": "eastus", "properties": {}})
    if code not in (200, 201, 202):
        sys.exit(f"database create failed: {code} {body}")
    return port


if __name__ == "__main__":
    port = provision()
    print(f"export SQL_HOST=localhost SQL_PORT={port} SQL_DB={DB} SQL_USER={USER} SQL_PASSWORD='{PASSWORD}'")
````
### Example 6 — `requirements-dev.txt`

````text
pytest
sqlglot
pyodbc
````
### Example 7 — `requirements.txt`

````text
pyodbc
````
### Example 8 — `schema.py`

````python
"""The warehouse is defined ONCE, as data. CREATE TABLEs and load statements are generated from it, so they cannot drift apart.
Two tables are enough to carry the pattern: a dimension (full refresh) and a fact (stage -> delete by key -> insert)."""
SPEC = {
    "dim_plan": [("plan_key", "BIGINT NOT NULL"), ("plan_id", "NVARCHAR(10)"), ("plan_type", "NVARCHAR(12)"), ("coinsurance", "DECIMAL(4,2)")],
    "fact_claim": [("claim_id", "NVARCHAR(20) NOT NULL"), ("plan_key", "BIGINT"), ("billed_total", "DECIMAL(14,2)"),
                   ("plan_paid_total", "DECIMAL(14,2)"), ("source_file", "NVARCHAR(40)"), ("batch_id", "NVARCHAR(30)")],
}
DIMS = ["dim_plan"]
FACTS = {"fact_claim": "claim_id"}                                  # fact table -> delete key
PK = {"dim_plan": "plan_key", "fact_claim": "claim_id"}


def create_statements():
    """[(schema, table, CREATE TABLE ...)]. dw.* carry a primary key; stg.* (load staging) do not."""
    out = []
    for t, cols in SPEC.items():
        body = ", ".join(f"{c} {ty}" for c, ty in cols)
        out.append(("dw", t, f"CREATE TABLE dw.{t} ({body}, PRIMARY KEY ({PK[t]}))"))
        if t in FACTS:
            out.append(("stg", t, f"CREATE TABLE stg.{t} ({body.replace(' NOT NULL', '')})"))
    return out


def tsql_ddl():
    """Idempotent T-SQL: a schema/table is created only if it is missing, so `up` can run any number of times."""
    sql = [f"IF SCHEMA_ID('{s}') IS NULL EXEC('CREATE SCHEMA {s}')" for s in ("dw", "stg")]
    return sql + [f"IF OBJECT_ID('{s}.{t}', 'U') IS NULL {create}" for s, t, create in create_statements()]
````
### Example 9 — `tests/conftest.py`

````python
import pathlib
import sqlite3
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import schema  # noqa: E402


@pytest.fixture
def conn(tmp_path):
    """SQLite standing in for SQL Server: two attached databases play the schemas `dw` and `stg`."""
    c = sqlite3.connect(":memory:")
    for s in ("dw", "stg"):
        c.execute(f"ATTACH DATABASE '{tmp_path / (s + '.db')}' AS {s}")
    for _, _, create in schema.create_statements():
        c.execute(create)
    c.commit()
    return c
````
### Example 10 — `tests/test_sql.py`

````python
from decimal import Decimal

import pytest
import sqlglot

import connect
import load as ld
import schema


class Rec:                                                          # records statements instead of running them
    def __init__(self):
        self.sql, self.params = [], []

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self.params.append(params or [])

    def commit(self):
        pass

    def rollback(self):
        pass


def _dims_facts(n=2, batch="b-1"):
    dims = {"dim_plan": (ld.columns("dim_plan"), [[1, "PL01", "HMO", Decimal("0.30")], [2, "PL02", "PPO", Decimal("0.20")]])}
    rows = [[f"C-{i}", 1, Decimal("100.00"), Decimal("70.00"), "f.csv.gz", batch] for i in range(n)]
    return dims, {"fact_claim": (ld.columns("fact_claim"), rows)}


# ---- T-SQL validity (Azure SQL speaks T-SQL; SQLite does not) -------------------------------------------------
def test_every_loader_statement_parses_as_tsql():
    rec = Rec()
    dims, facts = _dims_facts()
    ld.load_batch(rec, dims, facts)
    stmts = [s.replace("?", "@p") for s in rec.sql] + [c for _, _, c in schema.create_statements()]
    assert len(stmts) >= 8
    for s in stmts:
        sqlglot.parse_one(s, read="tsql")                           # raises on a syntax error


def test_idempotent_ddl_wraps_a_valid_create_and_guards_every_object():
    ddl = schema.tsql_ddl()
    assert [s for s in ddl if s.startswith("IF SCHEMA_ID")] and all(s.startswith("IF ") for s in ddl)
    assert len(ddl) == 2 + len(schema.create_statements())
    for s in ddl:
        if "CREATE TABLE" in s:
            sqlglot.parse_one(s[s.index("CREATE TABLE"):], read="tsql")


def test_insert_respects_sql_server_parameter_limit():
    rec = Rec()
    ld.insert_rows(rec, "dw.t", ["a", "b", "c", "d"], [["1"] * 4] * 1500)
    assert max(len(p) for p in rec.params) <= 2100 and sum(len(p) for p in rec.params) == 6000


def test_odbc_string_quotes_awkward_passwords():
    s = connect.odbc_string("localhost", 1433, "demo", "sa", "p;w}d=!")
    assert "PWD={p;w}}d=!};" in s and "SERVER=localhost,1433" in s and "TrustServerCertificate=yes" in s


# ---- behaviour (the same SQL, executed) -----------------------------------------------------------------------
def count(conn, sql):
    return conn.execute(sql).fetchone()[0]


def test_load_then_reconcile(conn):
    dims, facts = _dims_facts(3)
    ld.load_batch(conn, dims, facts)
    assert ld.reconcile(conn, "b-1", {"fact_claim": 3}, "210.00") == {"loaded": {"fact_claim": 3}, "plan_paid": "210.00"}


def test_reload_replaces_never_duplicates(conn):
    dims, facts = _dims_facts(3)
    ld.load_batch(conn, dims, facts)
    ld.load_batch(conn, dims, facts)
    assert count(conn, "SELECT COUNT(*) FROM dw.fact_claim") == 3 and count(conn, "SELECT COUNT(*) FROM dw.dim_plan") == 2


def test_redelivered_row_replaces_its_old_version(conn):
    paid = lambda: float(count(conn, "SELECT plan_paid_total FROM dw.fact_claim WHERE claim_id = 'C-0'"))
    dims, facts = _dims_facts(1)
    ld.load_batch(conn, dims, facts)
    assert paid() == 70.0
    facts["fact_claim"][1][0][3] = Decimal("999.00")                # same claim_id, new plan_paid_total (column index 3)
    ld.load_batch(conn, dims, facts)
    assert paid() == 999.0 and count(conn, "SELECT COUNT(*) FROM dw.fact_claim") == 1     # replaced, not appended


def test_failure_mid_load_rolls_back_dimensions_and_facts(conn, monkeypatch):
    real = ld.insert_rows

    def boom(cur, table, *a, **k):
        if table == "stg.fact_claim":
            raise RuntimeError("simulated SQL outage mid-load")
        return real(cur, table, *a, **k)

    monkeypatch.setattr(ld, "insert_rows", boom)
    dims, facts = _dims_facts()
    with pytest.raises(RuntimeError):
        ld.load_batch(conn, dims, facts)
    assert count(conn, "SELECT COUNT(*) FROM dw.dim_plan") == 0 and count(conn, "SELECT COUNT(*) FROM dw.fact_claim") == 0   # dimensions rolled back too


def test_reconcile_mismatch_is_a_loud_failure(conn):
    dims, facts = _dims_facts(2)
    ld.load_batch(conn, dims, facts)
    with pytest.raises(ld.ReconciliationError):
        ld.reconcile(conn, "b-1", {"fact_claim": 3}, "140.00")      # transform claims 3 rows; warehouse has 2
    with pytest.raises(ld.ReconciliationError):
        ld.reconcile(conn, "b-1", {"fact_claim": 2}, "141.00")      # the right count but the wrong money
````
### Example 11 — On real Azure (Terraform and auth)

- Terraform: `azurerm_mssql_server` + `azurerm_mssql_database` (serverless `GP_S_Gen5_1`, `auto_pause_delay_in_minutes = 60`).
- Auth: the function's identity / Entra instead of `sa`; the SQL password is a Key Vault reference ([guide 05](05-key-vault.md)). Valid certificate, so no `TrustServerCertificate`.
- Network: private endpoint rather than a public server.

---

## Reference

Project files:

```
connect.py
demo.py
docker-compose.yml
load.py
provision.py
requirements-dev.txt
requirements.txt
schema.py
tests/conftest.py
tests/test_sql.py
```

AWS counterpart: [09 RDS Postgres](../floci-aws-guides/09-rds-postgres.md).
Related: [05 Key Vault](05-key-vault.md).
