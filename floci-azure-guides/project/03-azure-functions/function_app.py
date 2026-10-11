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
