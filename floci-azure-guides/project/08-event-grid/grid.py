"""Floci AZ Event Grid, over plain HTTP (no SDK needed).
   ARM (management)  PUT  /subscriptions/<s>/resourceGroups/<rg>/providers/Microsoft.EventGrid/topics/<name>
                     POST .../topics/<name>/listKeys
                     PUT  .../topics/<name>/providers/Microsoft.EventGrid/eventSubscriptions/<sub>
   Data plane        POST /<topic>-eventgrid/api/events      (header aeg-sas-key; the URL is keyed on the TOPIC name)
Scope limits (Floci AZ): custom topics + WEBHOOK destinations only. NO storage system topic, so we publish the BlobCreated event ourselves."""
import json
import os
import time
import urllib.error
import urllib.request

BASE = os.environ.get("FLOCI_ENDPOINT", "http://localhost:4577")
API = "api-version=2025-02-15"
TOPIC, SUB = "ikipay-blobs", "inbox-to-gate"
TOPIC_ID = f"/subscriptions/dev/resourceGroups/rg/providers/Microsoft.EventGrid/topics/{TOPIC}"
INBOX_SUBJECT_PREFIX = "/blobServices/default/containers/inbox/"


def http(method, url, body=None, headers=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {}


def create_topic():
    code, body = http("PUT", f"{BASE}{TOPIC_ID}?{API}", {"location": "eastus", "properties": {"inputSchema": "EventGridSchema"}})
    assert code in (200, 201), (code, body)
    return body["properties"]["endpoint"]


def list_key():
    code, keys = http("POST", f"{BASE}{TOPIC_ID}/listKeys?{API}")
    assert code == 200, (code, keys)
    return keys["key1"]


def subscription_body(hook_url):
    return {"properties": {
        "destination": {"endpointType": "WebHook", "properties": {"endpointUrl": hook_url}},
        "filter": {"subjectBeginsWith": INBOX_SUBJECT_PREFIX, "includedEventTypes": ["Microsoft.Storage.BlobCreated"]},   # layer 1 of the self-trigger guard
        "retryPolicy": {"maxDeliveryAttempts": 10, "eventTimeToLiveInMinutes": 1440}}}


def create_subscription(hook_url):
    code, body = http("PUT", f"{BASE}{TOPIC_ID}/providers/Microsoft.EventGrid/eventSubscriptions/{SUB}?{API}", subscription_body(hook_url))
    assert code in (200, 201), (code, body)       # fails if the webhook did not echo the validation code: is hook_server.py running?
    return body


def blob_created(container, name):
    return {"id": f"evt-{container}-{name}", "eventType": "Microsoft.Storage.BlobCreated", "dataVersion": "1", "data": {"api": "PutBlob"},
            "subject": f"/blobServices/default/containers/{container}/blobs/{name}", "eventTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def publish(endpoint, key, events):
    code, body = http("POST", endpoint, events, {"aeg-sas-key": key})
    assert code in (200, 202), (code, body)
