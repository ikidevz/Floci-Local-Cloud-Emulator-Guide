# 05 — Key Vault

> [Index](README.md) · [← 04 — Azure SQL Database](04-azure-sql-database.md) · [06 — Event Hubs →](06-event-hubs.md)

Holds the two secrets the batch pipeline needs - the PII tokenisation salt and the SQL password - so no secret lives in code or app config.

---

## 1. Documentation

### Overview

- **Azure Key Vault (secrets)** — Holds the two secrets the batch pipeline needs - the PII tokenisation salt and the SQL password - so no secret lives in code or app config.

**Where the three pipelines use it**

| Pipeline | Role | Pattern that matters |
|---|---|---|
| 1 Batch ETL | `pii-salt` and `sql-password` secrets; the Function reads them as app settings | Key Vault *references* on real Azure; resolved values injected locally |

### At a glance

| Item | Value |
|------|-------|
| Python packages | `azure-keyvault-secrets==4.*`, `azure-identity==1.*` |
| Dev / test packages | `pytest` |
| Floci AZ ports published | `4577` |
| Docker socket | not required |
| Project folder | `project/05-key-vault/` |
| Tests | 4 (+2 Floci-only, skipped) |
| Verified against | a **fake vault** that performs the real 401 challenge handshake |

### How it behaves on Floci

- **Endpoint:** `http://localhost:4577/<account>-keyvault`.
- **Emulated:** secrets CRUD, **immutable versions**, soft-delete / recover / purge, property updates, plus keys, crypto operations and self-signed certificates (not needed here).
- **SDK quirk:** the Python SDK *enforces https* and a *challenge* auth flow, so you need a fake credential, an https->http transport and `verify_challenge_resource=False`.
- **Not emulated:** Key Vault *references* in function app settings (Floci does not resolve `@Microsoft.KeyVault(...)`).

Source: Floci AZ docs, `https://floci.io/floci-az/services/`.

### Key concepts

What the minimal project proves, and where in the code:

| Pattern | Why the pipelines need it | Where |
|---|---|---|
| A client that works against the emulator | The SDK refuses plain http and bearer tokens over it | `kv.client` |
| Versioning and rotation | Rotate a secret without breaking readers pinned to a version | `demo.py`, tests |
| Secrets resolved into app settings | What a Key Vault reference does for you on Azure | `app_settings_from_vault` |
| The exact production reference syntax | What the Terraform writes into the app | `kv_reference` |
| One code path, two environments | `VAULT_URL` switches to the real vault + managed identity | `kv.client` |

### Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| `Bearer token authentication is not permitted for non-TLS` | Transport mutates the URL for good | Use the restoring `ForceHttpTransport` in `kv.py` |
| `ChallengeResource` verification error | SDK checks the vault host | `verify_challenge_resource=False` (already set) |
| `@Microsoft.KeyVault(...)` arrives as a literal string | Floci does not resolve references | Inject resolved values at deploy time (`app_settings_from_vault`) |
| `BadParameter` on `nbf`/`exp` | Must be numeric Unix timestamps | Use the SDK's `expires_on` / `not_before` datetimes (it sends numeric timestamps) |

### What was verified

**Ran and passed (build sandbox):**

- 4 tests against a **fake vault** that performs the real Key Vault 401 *challenge* handshake: the client survives it, the fake credential is used afterwards, set/get/versions work, app settings resolve, the reference string is exact.
- **A real bug found and fixed:** Floci's documented transport rewrites the URL to http *permanently*. The SDK re-checks for https on every attempt, so the retry after a 401 challenge raised `ServiceRequestError`. `ForceHttpTransport` now restores the URL after sending.

**Not run:**

- The 2 `test_floci_*` tests (set/get/versioning and soft-delete -> recover -> purge) need a running Floci AZ and are skipped here. The fake vault mimics what real Key Vault does, not necessarily every response Floci gives.

---

## 2. Basic Usage

> Prerequisite: [guide 00](00-setup-floci.md) read once. Commands start from this guides folder (`floci-azure-guides/`); the project lives in `project/05-key-vault/`.

### Project structure

```text
project/05-key-vault/
  demo.py
  docker-compose.yml
  kv.py
  requirements-dev.txt
  requirements.txt
  tests/
    conftest.py
    test_kv.py
```

### Run it

```bash
cd project/05-key-vault && pip install -r requirements-dev.txt
docker compose up -d
python demo.py                 # seed, rotate, read an old version, resolve app settings
pytest tests -v                # 4 run anywhere; 2 need Floci and skip without it
```

---

## 3. Examples

Every file below is the exact content of `project/05-key-vault/`.

### Example 1 — `docker-compose.yml`

````yaml
services:
  floci-az:
    image: floci/floci-az:latest
    ports:
      - "4577:4577"          # Key Vault lives at  http://localhost:4577/<account>-keyvault
````
### Example 2 — `demo.py`

````python
"""python demo.py -> seed the two secrets, rotate one, read an old version, resolve app settings."""
import kv

c = kv.client()
c.set_secret("pii-salt", "dev-only-salt")
v1 = c.set_secret("sql-password", "Str0ng!Passw0rd")
v2 = c.set_secret("sql-password", "N3w!Passw0rd-rotated")                 # rotation = a NEW immutable version
assert c.get_secret("sql-password").value == "N3w!Passw0rd-rotated"        # 'latest' follows the newest version
assert c.get_secret("sql-password", v1.properties.version).value == "Str0ng!Passw0rd"   # the old one is still addressable
print("versions:", [p.version[:8] for p in c.list_properties_of_secret_versions("sql-password")])

print("app settings the Function would receive:", kv.app_settings_from_vault(c, {"PII_SALT": "pii-salt", "SQL_PASSWORD": "sql-password"}))
print("on real Azure that setting is instead:   ", kv.kv_reference("kv-demo-prod", "sql-password"))
````
### Example 3 — `kv.py`

````python
"""Key Vault for the pipelines' two secrets: the PII tokenisation salt and the SQL password.

Locally the Python SDK needs three workarounds (Floci AZ docs): the SDK ENFORCES https and a challenge-based auth flow, so we
  1. give it a fake credential,   2. rewrite https->http in a transport,   3. turn off challenge-resource verification.
On real Azure set VAULT_URL=https://<vault>.vault.azure.net and the same code uses the Function's managed identity.
"""
import os
import re
import time

from azure.core.credentials import AccessToken, TokenCredential
from azure.core.pipeline.transport import RequestsTransport
from azure.keyvault.secrets import SecretClient

ACCOUNT = "devstoreaccount1"


class FakeCredential(TokenCredential):
    def get_token(self, *scopes, **kwargs):
        return AccessToken("fake-token", int(time.time()) + 3600)


class ForceHttpTransport(RequestsTransport):
    """Rewrite https->http for the wire only. The URL is RESTORED afterwards: the SDK's auth policy re-checks for https on every
    attempt, so a permanent rewrite breaks the retry that follows a 401 challenge (Floci's own snippet only works if no challenge is issued)."""
    def send(self, request, **kwargs):
        original = request.url
        request.url = original.replace("https://", "http://", 1)
        try:
            return super().send(request, **kwargs)
        finally:
            request.url = original


def client(endpoint=None, **kw):
    vault_url = os.environ.get("VAULT_URL")
    if vault_url:                                                      # real Azure: no secret anywhere, the identity is the credential
        from azure.identity import DefaultAzureCredential
        return SecretClient(vault_url, DefaultAzureCredential(), **kw)
    base = endpoint or os.environ.get("FLOCI_ENDPOINT", "http://localhost:4577")
    return SecretClient(vault_url=re.sub(r"^http://", "https://", base) + f"/{ACCOUNT}-keyvault", credential=FakeCredential(),
                        transport=ForceHttpTransport(), verify_challenge_resource=False, **kw)


def app_settings_from_vault(c, mapping):
    """{ENV_VAR: secret-name} -> {ENV_VAR: value}. This is what a Key Vault REFERENCE does for you on real Azure; Floci AZ does not
    resolve references, so locally the deploy script injects the resolved values as the function app's environment."""
    return {env: c.get_secret(secret).value for env, secret in mapping.items()}


def kv_reference(vault_name, secret):
    """The app-setting VALUE real Azure resolves at runtime (what the Terraform in the pipelines writes). Nothing is stored in the app."""
    return f"@Microsoft.KeyVault(SecretUri=https://{vault_name}.vault.azure.net/secrets/{secret})"
````
### Example 4 — `requirements-dev.txt`

````text
azure-keyvault-secrets==4.*
pytest
````
### Example 5 — `requirements.txt`

````text
azure-keyvault-secrets==4.*
azure-identity==1.*          # only used against real Azure (managed identity); Floci AZ uses a fake credential
````
### Example 6 — `tests/conftest.py`

````python
import json
import pathlib
import re
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import kv  # noqa: E402


class FakeVault(BaseHTTPRequestHandler):
    """Just enough Key Vault to exercise OUR client wiring: the 401 challenge handshake real Key Vault performs, then set/get/versions."""
    store, seen_auth = {}, []

    def log_message(self, *a):
        pass

    def _send(self, code, body=None, headers=None):
        raw = json.dumps(body).encode() if body is not None else b""
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _bundle(self, name, ver, rec):
        return {"value": rec["value"], "id": f"https://{self.headers['Host']}/secrets/{name}/{ver}",
                "attributes": {"enabled": True, "created": rec["t"], "updated": rec["t"]}}

    def _route(self, method):
        auth = self.headers.get("Authorization")
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else b""
        if not auth:                                                   # real Key Vault: unauthenticated first request -> 401 + challenge
            return self._send(401, {"error": {"code": "Unauthorized"}}, {"WWW-Authenticate": 'Bearer authorization="https://login.example/tenant", resource="https://vault.azure.net"'})
        FakeVault.seen_auth.append(auth)
        m = re.match(r"^/devstoreaccount1-keyvault/secrets/([^/?]+)(?:/(versions|[0-9a-f]{32}))?", self.path)
        if not m:
            return self._send(404, {"error": {"code": "NotFound"}})
        name, tail = m.groups()
        vers = FakeVault.store.setdefault(name, [])
        if method == "PUT":
            rec = {"value": json.loads(body)["value"], "t": 1700000000 + len(vers)}
            ver = uuid.uuid4().hex
            vers.append((ver, rec))
            return self._send(200, self._bundle(name, ver, rec))
        if method == "GET" and tail == "versions":
            return self._send(200, {"value": [{"id": f"https://{self.headers['Host']}/secrets/{name}/{v}", "attributes": {"enabled": True, "created": r["t"]}} for v, r in vers]})
        if method == "GET" and vers:
            ver, rec = next(((v, r) for v, r in vers if v == tail), None) if tail else vers[-1]
            return self._send(200, self._bundle(name, ver, rec))
        return self._send(404, {"error": {"code": "SecretNotFound"}})

    def do_PUT(self):
        self._route("PUT")

    def do_GET(self):
        self._route("GET")


@pytest.fixture
def fake_vault_client(monkeypatch):
    monkeypatch.delenv("VAULT_URL", raising=False)
    FakeVault.store, FakeVault.seen_auth = {}, []
    srv = HTTPServer(("127.0.0.1", 0), FakeVault)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield kv.client(endpoint=f"http://127.0.0.1:{srv.server_port}")
    srv.shutdown()


@pytest.fixture(scope="session")
def floci_client():
    """The real thing: auto-skips when Floci AZ is not answering."""
    c = kv.client(retry_total=0, connection_timeout=2)
    try:
        list(c.list_properties_of_secrets())
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Floci AZ Key Vault not reachable ({type(e).__name__}); run `docker compose up -d`")
    return c
````
### Example 7 — `tests/test_kv.py`

````python
import uuid

import kv


def versioning_contract(c, name):
    """Identical assertions for the fake vault (this sandbox) and Floci AZ (your machine)."""
    v1 = c.set_secret(name, "one")
    v2 = c.set_secret(name, "two")
    assert v1.properties.version != v2.properties.version
    assert c.get_secret(name).value == "two"                                   # latest wins
    assert c.get_secret(name, v1.properties.version).value == "one"            # old version still addressable
    assert len(list(c.list_properties_of_secret_versions(name))) == 2


def test_client_survives_the_challenge_handshake_with_http_rewrite(fake_vault_client):
    from conftest import FakeVault
    versioning_contract(fake_vault_client, "pii-salt")
    assert FakeVault.seen_auth and FakeVault.seen_auth[0] == "Bearer fake-token"   # the fake credential was used after the 401 challenge


def test_app_settings_resolve_from_vault(fake_vault_client):
    fake_vault_client.set_secret("pii-salt", "s1")
    fake_vault_client.set_secret("sql-password", "p1")
    assert kv.app_settings_from_vault(fake_vault_client, {"PII_SALT": "pii-salt", "SQL_PASSWORD": "sql-password"}) == {"PII_SALT": "s1", "SQL_PASSWORD": "p1"}


def test_kv_reference_has_the_exact_azure_syntax():
    assert kv.kv_reference("kv-x", "sql-password") == "@Microsoft.KeyVault(SecretUri=https://kv-x.vault.azure.net/secrets/sql-password)"


def test_vault_url_env_switches_to_real_azure(monkeypatch):
    monkeypatch.setenv("VAULT_URL", "https://kv-x.vault.azure.net")
    try:
        kv.client()
    except ModuleNotFoundError as e:                                           # azure-identity is only needed on real Azure
        assert "azure" in str(e)


# --- against the real emulator (skipped here unless Floci AZ is up) -----------------------------------------------
def test_floci_versioning(floci_client):
    versioning_contract(floci_client, f"s-{uuid.uuid4().hex[:8]}")


def test_floci_soft_delete_recover_purge(floci_client):
    name = f"s-{uuid.uuid4().hex[:8]}"
    floci_client.set_secret(name, "x")
    assert floci_client.begin_delete_secret(name).result().name == name
    assert floci_client.begin_recover_deleted_secret(name).result().name == name
    assert floci_client.get_secret(name).value == "x"
    floci_client.begin_delete_secret(name).result()
    floci_client.purge_deleted_secret(name)
````
### Example 8 — On real Azure (Terraform and auth)

- Terraform: `azurerm_key_vault` (`rbac_authorization_enabled = true`) + `azurerm_key_vault_secret`.
- App setting becomes `@Microsoft.KeyVault(SecretUri=...)`; the function's identity holds *Key Vault Secrets User* and nothing else.
- The salt is long-lived: rotating it re-tokenises every member.

---

## Reference

Project files:

```
demo.py
docker-compose.yml
kv.py
requirements-dev.txt
requirements.txt
tests/conftest.py
tests/test_kv.py
```
