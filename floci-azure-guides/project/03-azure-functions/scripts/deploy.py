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
