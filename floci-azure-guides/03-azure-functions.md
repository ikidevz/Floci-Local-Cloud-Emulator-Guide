# 03 — Azure Functions

> [Index](README.md) · [← 02 — Table Storage](02-table-storage.md) · [04 — Azure SQL Database →](04-azure-sql-database.md)

The compute in all three pipelines: one HTTP-triggered function per pipeline, deployed as a vendored ZIP and invoked over HTTP.

---

## 1. Documentation

### Overview

- **Azure Functions (Python v2 model)** — The compute in all three pipelines: one HTTP-triggered function per pipeline, deployed as a vendored ZIP and invoked over HTTP.

**Where the three pipelines use it**

| Pipeline | Role | Pattern that matters |
|---|---|---|
| 1 Batch ETL | `run_nightly` runs plan -> conform -> transform -> load -> reconcile | HTTP always; Timer only on real Azure (`ENABLE_TIMER=1`) |
| 2 Streaming | `process_batch` processes one Event Hub batch | HTTP always; Event Hub trigger only on real Azure; a *bridge* feeds it locally |
| 3 DQ gate | `gate` is the Event Grid webhook (handshake + events) | HTTP **is** the webhook; non-2xx makes Event Grid retry |

### At a glance

| Item | Value |
|------|-------|
| Python packages | `azure-storage-blob==12.*` |
| Dev / test packages | `azure-functions`, `pytest` |
| Floci AZ ports published | `4577` |
| Docker socket | required |
| Project folder | `project/03-azure-functions/` |
| Tests | 4 |
| Verified against | unit tests; `make package` run, ZIP layout checked |

### How it behaves on Floci

- **Endpoint:** `http://localhost:4577/<account>-functions` - management under `/admin/apps/...`, invocation at `/api/<app>/<function>`.
- **Real runtime:** Floci starts the genuine `mcr.microsoft.com/azure-functions/python:4` container on first call and keeps it warm. **Needs the Docker socket mounted.**
- **Python v2 model supported:** a ZIP with `function_app.py` at its root is recognised; `host.json` is preserved; vendored packages go in `.python_packages/lib/site-packages`.
- **Not emulated:** non-HTTP triggers (Timer, Event Hub, Service Bus, Blob). Mocked mode returns a synthetic 200 without running your code.

Source: Floci AZ docs, `https://floci.io/floci-az/services/`.

### Key concepts

What the minimal project proves, and where in the code:

| Pattern | Why the pipelines need it | Where |
|---|---|---|
| HTTP trigger: 200 on success, **500 on failure** | Bridges, Event Grid and monitors all key off the status code | `function_app.run` |
| Non-HTTP triggers gated behind an env flag | Same code deploys to Floci (HTTP only) and to Azure (Timer/Event Hub) | `ENABLE_TIMER` |
| Deployable ZIP with vendored dependencies | The runtime container does not `pip install` for you | `Makefile` `package` |
| App settings reach the function | Connection strings, salt, topic names | `deploy.py` `environment` |
| Container -> emulator networking | The function runs in a container: `localhost` is itself | `host.docker.internal` in `deploy.py` |

### Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Deploy works but invocation hangs | Docker socket not mounted, or UFW blocks the bridge (native Linux) | Mount `/var/run/docker.sock`; `sudo ufw allow in on docker0` |
| `ModuleNotFoundError` inside the function | Dependencies not vendored for the runtime | `make package`: it vendors Linux wheels for the runtime (Debian 12, Python 3.12) even when you build on macOS/Windows; on an ARM host override `PLATFORMS` |
| Function cannot reach blob storage | `localhost` inside a container | `host.docker.internal` (or the `floci-az` hostname when everything is in one compose network) |
| The Timer never fires | Floci cannot fire timers | Start runs over HTTP; Timer exists only on real Azure |

### What was verified

**Ran and passed (build sandbox):**

- 4 unit tests: 200 path with a deterministic blob name, loud 500, missing-setting 500, Timer registered **only** when `ENABLE_TIMER=1`.
- `make package` executed: the vendoring command resolves Linux x86-64 wheels (including `cryptography`) and the ZIP has `function_app.py` + `host.json` at its root and the dependencies under `.python_packages`.

**Not run:**

- Deployment to Floci and invocation (`deploy.py` only compiled). The `handler` string `function_app.run` follows the uploaded blueprints; Floci's docs show only a Node example for `handler`, so check it on first deploy.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md) read once. Commands start from this guides folder (`floci-azure-guides/`); the project lives in `project/03-azure-functions/`.

### Project structure

```text
project/03-azure-functions/
  Makefile
  docker-compose.yml
  floci.py
  function_app.py
  host.json
  requirements-dev.txt
  requirements.txt
  scripts/
    deploy.py
  tests/
    test_function_app.py
```

### Run it

```bash
cd project/03-azure-functions && pip install -r requirements-dev.txt
pytest tests -v                          # logic only, no Docker needed
docker compose up -d
make package                             # build/demo_fn.zip (Linux x86-64 wheels). ARM host: see the Makefile comment
python scripts/deploy.py deploy          # PUT app + PUT function
python scripts/deploy.py check           # 200 / app settings / blob written / failure -> 500
```

---

## 3. Examples

Every file below is the exact content of `project/03-azure-functions/`.

### Example 1 — `docker-compose.yml`

````yaml
services:
  floci-az:
    image: floci/floci-az:latest
    ports:
      - "4577:4577"                                  # Functions admin + invoke: http://localhost:4577/<account>-functions
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock    # REQUIRED: Floci spawns the real Azure Functions runtime as a container
````
### Example 2 — `Makefile`

````make
# Function zip = function_app.py + host.json + vendored dependencies (the runtime container has NO internet-installed packages).
# The runtime image is Linux x86-64 Debian 12. On an ARM host:  make package PLATFORMS="--platform manylinux_2_28_aarch64 --platform manylinux2014_aarch64"
PLATFORMS ?= --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64
PY ?= python

package:
	rm -rf build && mkdir -p build/app/.python_packages/lib/site-packages
	$(PY) -m pip install -r requirements.txt -t build/app/.python_packages/lib/site-packages $(PLATFORMS) --only-binary=:all: --python-version 3.12 --implementation cp
	cp function_app.py host.json build/app/
	cd build/app && $(PY) -m zipfile -c ../demo_fn.zip . && cd ../.. && ls -l build/demo_fn.zip
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
### Example 4 — `function_app.py`

````python
"""Azure Functions v2 (Python). The shape every pipeline uses:
   * an HTTP trigger (the ONLY trigger Floci AZ can invoke)
   * clients built from environment variables (connection string locally, managed identity on Azure)
   * success -> 200 JSON;  any exception -> loud 500 JSON (callers/Event Grid/bridges retry on non-2xx)
   * non-HTTP triggers registered ONLY on real Azure, behind an env flag
"""
import datetime
import json
import os

import azure.functions as func

app = func.FunctionApp()


def _blob():
    from azure.storage.blob import BlobServiceClient            # imported lazily: vendored in .python_packages
    return BlobServiceClient.from_connection_string(os.environ["STORAGE_CONNECTION_STRING"])


def _reply(body, status=200):
    return func.HttpResponse(json.dumps(body), status_code=status, mimetype="application/json")


@app.route(route="run", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def run(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json() if req.get_body() else {}
        if body.get("fail"):                                     # demo hook: prove the failure path returns 500
            raise ValueError("simulated failure")
        name = str(body.get("name", "world"))
        out = {"hello": name, "at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "marker": os.environ.get("MY_MARKER", "")}
        svc = _blob()
        c = svc.get_container_client("fn-out")
        if not c.exists():
            c.create_container()
        c.get_blob_client(f"hello-{name}.json").upload_blob(json.dumps(out).encode(), overwrite=True)    # deterministic name
        return _reply({"status": "OK", **out})
    except Exception as e:                                       # noqa: BLE001
        return _reply({"status": "FAILED", "error": f"{type(e).__name__}: {e}"}, 500)


if os.environ.get("ENABLE_TIMER") == "1":                        # set on real Azure only; Floci AZ does not fire timers
    @app.timer_trigger(schedule="0 30 18 * * *", arg_name="timer", run_on_startup=False)
    def nightly(timer: func.TimerRequest) -> None:
        pass                                                     # a real pipeline calls its orchestrator here
````
### Example 5 — `host.json`

````json
{ "version": "2.0", "functionTimeout": "00:05:00", "extensions": { "http": { "routePrefix": "api" } } }
````
### Example 6 — `requirements-dev.txt`

````text
azure-functions
azure-storage-blob==12.*
pytest
````
### Example 7 — `requirements.txt`

````text
azure-storage-blob==12.*
````
### Example 8 — `scripts/deploy.py`

````python
"""python scripts/deploy.py deploy | invoke [--fail] | check      (run `make package` first)

Floci AZ Functions management API:
    PUT  /<acct>-functions/admin/apps/<app>                   create app (+ environment variables)
    PUT  /<acct>-functions/admin/apps/<app>/functions/<fn>    deploy ZIP (base64)
    POST /<acct>-functions/api/<app>/<fn>                     invoke (HTTP triggers only)
The function runs in a CONTAINER, so it reaches the emulator at host.docker.internal, never localhost.
"""
import base64
import json
import pathlib
import sys
import urllib.error
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import floci  # noqa: E402

BASE = f"http://localhost:4577/{floci.ACCOUNT}-functions"
APP, FN = "demo-app", "run"


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read() or b"{}"
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"raw": raw.decode(errors="replace")[:300]}


def deploy():
    env = {"STORAGE_CONNECTION_STRING": floci.conn_str(host="host.docker.internal"), "MY_MARKER": "from-app-settings"}
    print(call("PUT", f"/admin/apps/{APP}", {"runtime": "python", "linuxFxVersion": "Python|3.12", "environment": env}))
    z = base64.b64encode(pathlib.Path("build/demo_fn.zip").read_bytes()).decode()
    print(call("PUT", f"/admin/apps/{APP}/functions/{FN}", {"handler": "function_app.run", "timeoutSeconds": 120, "zipBase64": z}))


def invoke(payload):
    code, body = call("POST", f"/api/{APP}/{FN}", payload)
    print(code, json.dumps(body)[:400])
    return code, body


def check():
    from azure.storage.blob import BlobServiceClient
    ok_code, ok = invoke({"name": "floci"})
    bad_code, _ = invoke({"fail": True})
    blob = BlobServiceClient.from_connection_string(floci.conn_str()).get_blob_client("fn-out", "hello-floci.json")
    results = {"HTTP 200 on success": ok_code == 200, "app settings reach the function": ok.get("marker") == "from-app-settings",
               "function wrote a blob through host.docker.internal": blob.exists(), "failure is a loud HTTP 500": bad_code == 500}
    for k, v in results.items():
        print("PASS" if v else "FAIL", k)
    sys.exit(0 if all(results.values()) else 1)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    {"deploy": deploy, "invoke": lambda: invoke({"fail": True} if "--fail" in sys.argv else {"name": "world"}), "check": check}[cmd]()
````
### Example 9 — `tests/test_function_app.py`

````python
import json
import pathlib
import sys

import azure.functions as func

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))


def load(monkeypatch, **env):
    monkeypatch.delenv("ENABLE_TIMER", raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    sys.modules.pop("function_app", None)
    import function_app
    return function_app


def call(fa, body=None):
    req = func.HttpRequest("POST", "/api/run", body=json.dumps(body).encode() if body is not None else b"")
    return fa.run.build().get_user_function()(req)


class FakeBlob:
    def __init__(self):
        self.store = {}

    def get_container_client(self, name):
        outer = self

        class C:
            def exists(self):
                return True

            def get_blob_client(self, blob):
                class B:
                    def upload_blob(self, data, overwrite=False):
                        outer.store[(name, blob)] = data
                return B()
        return C()


def test_http_trigger_returns_200_and_writes_deterministic_blob(monkeypatch):
    fa = load(monkeypatch, MY_MARKER="m")
    fake = FakeBlob()
    monkeypatch.setattr(fa, "_blob", lambda: fake)
    r = call(fa, {"name": "x"})
    assert r.status_code == 200 and json.loads(r.get_body())["marker"] == "m"
    assert ("fn-out", "hello-x.json") in fake.store                         # same input -> same blob name -> replay-safe


def test_failure_is_a_loud_500_not_a_silent_success(monkeypatch):
    fa = load(monkeypatch)
    monkeypatch.setattr(fa, "_blob", lambda: FakeBlob())
    r = call(fa, {"fail": True})
    assert r.status_code == 500 and json.loads(r.get_body())["status"] == "FAILED"


def test_missing_setting_is_a_500(monkeypatch):
    fa = load(monkeypatch)
    monkeypatch.delenv("STORAGE_CONNECTION_STRING", raising=False)
    assert call(fa, {"name": "x"}).status_code == 500


def test_timer_is_registered_only_when_enabled(monkeypatch):
    def names(fa):
        return {f.get_function_name() for f in fa.app.get_functions()}
    assert names(load(monkeypatch)) == {"run"}                              # what Floci AZ sees
    assert names(load(monkeypatch, ENABLE_TIMER="1")) == {"run", "nightly"}  # what real Azure sees
````
### Example 10 — On real Azure (Terraform and auth)

- Terraform: `azurerm_service_plan` (`Y1` Consumption) + `azurerm_linux_function_app` with a system-assigned identity and Application Insights.
- Triggers: set `ENABLE_TIMER=1`/`ENABLE_EVENTHUB_TRIGGER=1`; the webhook becomes `AuthLevel.FUNCTION` (key in the Event Grid URL).
- A failed-execution alert (`azurerm_monitor_scheduled_query_rules_alert_v2`) pages on-call.

---

## Reference

Project files:

```
Makefile
docker-compose.yml
floci.py
function_app.py
host.json
requirements-dev.txt
requirements.txt
scripts/deploy.py
tests/test_function_app.py
```

AWS counterpart: [02 Lambda](../floci-aws-guides/02-lambda.md).
