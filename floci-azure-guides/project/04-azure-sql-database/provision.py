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
