# 02 — Lambda

> [Index](README.md) · [← 01 — S3](01-s3.md) · [03 — Glue (Catalog, Jobs, Crawlers) →](03-glue.md)

Three functions power the pipeline: `ingest` (SQS → DynamoDB/Kinesis), `glue-runner` (drives Glue for Step Functions) and `kinesis-to-firehose` (bridge).

---

## 1. Documentation

### Overview

- **`ingest`**: triggered by SQS (messages created by S3 notifications) or invoked directly. Writes a run log to DynamoDB and optionally publishes to Kinesis.
- **`glue-runner`**: lets Step Functions drive Glue (start/poll jobs and crawlers) without a native integration.
- **`kinesis-to-firehose`**: bridges a Kinesis stream into Firehose.

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `lambda` |
| Terraform endpoint | `lambda` |
| Needs first | an IAM role ([07](07-iam.md)); a table, queue or stream if the function uses them |
| Docker socket | **required** (functions run as real containers) |

### How it behaves on Floci

- Functions run in **real Docker containers** (official `public.ecr.aws/lambda` images), so mount the Docker socket. The first invoke is slow while the image is pulled.
- With `FLOCI_HOSTNAME` set, Floci injects `AWS_ENDPOINT_URL` into the function container. boto3 reads that variable automatically, so handler code needs **no endpoint logic** and is identical on real AWS.
- Wait for `State == Active` and `LastUpdateStatus == Successful` before invoking (`wait_active` in Example 2).
- Package files at the zip root: `handler.py`, not `ingest/handler.py`.

### Key concepts

- **Handler**: `handler.lambda_handler` means file `handler.py`, function `lambda_handler`.
- **Event source mapping**: connects SQS or Kinesis to the function. Lambda polls for you.
- **Partial batch failure**: the handler reports only the failed messages (`batchItemFailures`); the rest of the batch is deleted.
- **Resource policy** (`add_permission`): lets S3, SNS or Events invoke the function.
- **Dead-letter config**: where asynchronous failures go (an SQS queue).

### Gotchas

- Return `batchItemFailures` and enable `ReportBatchItemFailures` so one bad SQS message does not retry the whole batch.
- Dependencies must be vendored into the zip (`pip install -t lambda_src/ingest -r requirements.txt`).
- `source_code_hash` makes Terraform redeploy when the code changes.

---

## 2. Basic Usage

> Prerequisite: Floci is running with the Docker socket mounted, and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

```python
import io, json, zipfile
import boto3

iam, lam = boto3.client("iam"), boto3.client("lambda")

# 1. A role Lambda can assume (see guide 07 for least-privilege policies)
trust = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
role_arn = iam.create_role(RoleName="demo-lambda-role",
                           AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]

# 2. Package a handler. The file sits at the zip root.
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w") as z:
    z.writestr("handler.py",
               "def lambda_handler(event, context):\n"
               "    return {'hello': event.get('name', 'world')}\n")

# 3. Create the function and wait until it is Active
lam.create_function(FunctionName="demo-hello", Runtime="python3.12", Role=role_arn,
                    Handler="handler.lambda_handler", Code={"ZipFile": buf.getvalue()})
lam.get_waiter("function_active_v2").wait(FunctionName="demo-hello")
```

```python
# (needs Floci: runs the function in a Docker container; the first call pulls the image)
resp = lam.invoke(FunctionName="demo-hello", Payload=json.dumps({"name": "floci"}).encode())
print(json.loads(resp["Payload"].read()))        # {'hello': 'floci'}
```

---

## 3. Examples

### Example 1 — Package a directory into a zip

`src/lambda_ops/packaging.py`

```python
import io
import zipfile
from pathlib import Path


def zip_dir(src_dir: str) -> bytes:
    """Zip a directory with files at the archive root (handler.py must not be nested)."""
    buf = io.BytesIO()
    root = Path(src_dir)
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(root.rglob("*")):
            if p.is_file() and "__pycache__" not in p.parts:
                zf.write(p, p.relative_to(root))
    return buf.getvalue()
```

### Example 2 — Create/update, wait, invoke, permission

`deploy` creates the function or updates it if it exists, and `wait_active` handles cold starts.

`src/lambda_ops/functions.py`

```python
import json

from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until
from src.lambda_ops.packaging import zip_dir

lam = client("lambda")


def wait_active(fn_name: str, timeout: int = 180) -> None:
    """Wait for State=Active and LastUpdateStatus=Successful (cold start pulls a runtime image)."""
    def status() -> str:
        cfg = lam.get_function(FunctionName=fn_name)["Configuration"]
        state = cfg.get("State", "Active")
        if state != "Active":
            return state
        update = cfg.get("LastUpdateStatus", "Successful")
        return "Active" if update == "Successful" else update

    wait_until(status, done={"Active"}, failed={"Failed"}, timeout=timeout, interval=2)


def deploy(fn_name: str, src_dir: str, role_arn: str, handler: str = "handler.lambda_handler",
           runtime: str = "python3.12", env: dict | None = None, timeout: int = 30,
           memory: int = 256, dlq_arn: str | None = None) -> str:
    """Create or update a function from a source directory. Returns the function ARN."""
    code = zip_dir(src_dir)
    cfg = dict(Runtime=runtime, Role=role_arn, Handler=handler, Timeout=timeout, MemorySize=memory,
               Environment={"Variables": env or {}})
    if dlq_arn:
        cfg["DeadLetterConfig"] = {"TargetArn": dlq_arn}
    try:
        arn = lam.create_function(FunctionName=fn_name, Code={"ZipFile": code}, **cfg)["FunctionArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        lam.update_function_code(FunctionName=fn_name, ZipFile=code)
        wait_active(fn_name)
        arn = lam.update_function_configuration(FunctionName=fn_name, **cfg)["FunctionArn"]
    wait_active(fn_name)
    return arn


def invoke(fn_name: str, payload: dict, async_: bool = False) -> dict:
    resp = lam.invoke(FunctionName=fn_name, InvocationType="Event" if async_ else "RequestResponse",
                      Payload=json.dumps(payload).encode())
    if async_:
        return {"status": resp["StatusCode"]}
    body = json.loads(resp["Payload"].read() or b"null")
    if "FunctionError" in resp:
        raise RuntimeError(f"Lambda error: {body}")
    return body


def allow_service(fn_name: str, principal: str, source_arn: str, sid: str) -> None:
    """Resource policy so S3 / SNS / Events can invoke the function."""
    try:
        lam.add_permission(FunctionName=fn_name, StatementId=sid, Action="lambda:InvokeFunction",
                           Principal=principal, SourceArn=source_arn)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise


def delete(fn_name: str) -> None:
    try:
        lam.delete_function(FunctionName=fn_name)
    except lam.exceptions.ResourceNotFoundException:
        pass
```

Deploy and invoke:

```python
from src.common.aws import name
from src.iam_ops.roles import create_all
from src.lambda_ops.functions import deploy, invoke

role = create_all()["lambda-role"]
deploy(name("ingest"), "lambda_src/ingest", role)
print(invoke(name("ingest"), {"job": "smoke"}))   # {'statusCode': 200, 'body': 'job: smoke'}
```

### Example 3 — Event source mappings (SQS and Kinesis triggers)

`src/lambda_ops/mappings.py`

```python
from botocore.exceptions import ClientError

from src.common.aws import client

lam = client("lambda")


def sqs_trigger(fn_name: str, queue_arn: str, batch_size: int = 10) -> str:
    return lam.create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName=fn_name, BatchSize=batch_size,
        FunctionResponseTypes=["ReportBatchItemFailures"])["UUID"]


def kinesis_trigger(fn_name: str, stream_arn: str, batch_size: int = 100,
                    start: str = "LATEST") -> str:
    return lam.create_event_source_mapping(
        EventSourceArn=stream_arn, FunctionName=fn_name, BatchSize=batch_size,
        StartingPosition=start, MaximumRetryAttempts=2, BisectBatchOnFunctionError=True)["UUID"]


def list_mappings(fn_name: str) -> list[dict]:
    return lam.list_event_source_mappings(FunctionName=fn_name)["EventSourceMappings"]


def delete_mapping(uuid: str) -> None:
    try:
        lam.delete_event_source_mapping(UUID=uuid)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
```

### Example 4 — `ingest` handler (SQS → DynamoDB / Kinesis)

`lambda_src/ingest/handler.py`

```python
"""
Ingest Lambda. Triggered by SQS (messages created by S3 notifications) or invoked directly.

Env: TABLE_NAME (DynamoDB run log), STREAM_NAME (optional Kinesis stream for downstream consumers).
boto3 reads AWS_ENDPOINT_URL from the environment, which Floci injects into Lambda containers,
so no endpoint code is needed here.
"""
import json
import logging
import os
import time
import uuid

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_ddb = boto3.client("dynamodb")
_kinesis = boto3.client("kinesis")


def _log_event(run_id: str, status: str, detail: str) -> None:
    table = os.environ.get("TABLE_NAME")
    if not table:
        return
    _ddb.put_item(TableName=table, Item={
        "run_id": {"S": run_id},
        "ts": {"S": f"{time.time():.6f}-{uuid.uuid4().hex[:6]}"},
        "status": {"S": status},
        "detail": {"S": detail[:400]},
        "expires_at": {"N": str(int(time.time()) + 7 * 86400)},
    })


def _publish(record: dict) -> None:
    stream = os.environ.get("STREAM_NAME")
    if stream:
        _kinesis.put_record(StreamName=stream, Data=json.dumps(record).encode(),
                            PartitionKey=str(record.get("key", "default")))


def _handle_body(body: dict) -> str:
    """Process one logical message. Returns a short description. Raises on failure."""
    if body.get("fail"):
        raise ValueError("forced failure (used to test the DLQ)")

    # S3 sends an 's3:TestEvent' (no 'Records') when the notification is configured: ignore it.
    if body.get("Event") == "s3:TestEvent" or ("Records" not in body and "job" not in body):
        return "ignored"

    if "Records" in body:  # S3 event notification
        keys = []
        for r in body["Records"]:
            bucket = r["s3"]["bucket"]["name"]
            key = r["s3"]["object"]["key"]
            keys.append(f"s3://{bucket}/{key}")
            _publish({"key": key, "bucket": bucket, "event": r.get("eventName", "")})
        return "s3: " + ", ".join(keys)

    _publish({"key": body["job"], "payload": body})
    return f"job: {body['job']}"


def lambda_handler(event, context):
    run_id = getattr(context, "aws_request_id", None) or uuid.uuid4().hex

    # SQS batch: report only the failed messages so the rest are not retried
    if "Records" in event and event["Records"] and event["Records"][0].get("eventSource") == "aws:sqs":
        failures = []
        for rec in event["Records"]:
            try:
                desc = _handle_body(json.loads(rec["body"]))
                _log_event(run_id, "OK", desc)
            except Exception as exc:  # noqa: BLE001
                logger.error("message %s failed: %s", rec["messageId"], exc)
                _log_event(run_id, "FAILED", str(exc))
                failures.append({"itemIdentifier": rec["messageId"]})
        return {"batchItemFailures": failures}

    # Direct invoke (tests, Step Functions)
    desc = _handle_body(event)
    _log_event(run_id, "OK", desc)
    return {"statusCode": 200, "body": desc}
```

### Example 5 — `glue-runner` handler

`lambda_src/glue_runner/handler.py`

```python
"""
Glue runner Lambda: lets Step Functions drive Glue without the (unsupported-on-Floci) glue:startJobRun.sync.

event = {"action": "start_job" | "job_status" | "start_crawler" | "crawler_status", ...}
"""
import boto3

glue = boto3.client("glue")


def lambda_handler(event, context):
    action = event["action"]

    if action == "start_job":
        run_id = glue.start_job_run(JobName=event["job"], Arguments=event.get("arguments", {}))["JobRunId"]
        return {"job": event["job"], "run_id": run_id}

    if action == "job_status":
        run = glue.get_job_run(JobName=event["job"], RunId=event["run_id"])["JobRun"]
        return {"job": event["job"], "run_id": event["run_id"], "state": run["JobRunState"],
                "error": run.get("ErrorMessage", "")}

    if action == "start_crawler":
        glue.start_crawler(Name=event["crawler"])
        return {"crawler": event["crawler"]}

    if action == "crawler_status":
        c = glue.get_crawler(Name=event["crawler"])["Crawler"]
        return {"crawler": event["crawler"], "state": c["State"],
                "last_crawl": c.get("LastCrawl", {}).get("Status", "")}

    raise ValueError(f"unknown action: {action}")
```

### Example 6 — `kinesis-to-firehose` bridge handler

`lambda_src/kinesis_to_firehose/handler.py`

```python
"""Bridge: Kinesis stream -> Firehose (Floci has no KinesisStreamAsSource, and this works everywhere)."""
import base64
import os

import boto3

firehose = boto3.client("firehose")


def lambda_handler(event, context):
    stream = os.environ["FIREHOSE_STREAM"]
    records = []
    for rec in event.get("Records", []):
        data = base64.b64decode(rec["kinesis"]["data"])
        if not data.endswith(b"\n"):
            data += b"\n"
        records.append({"Data": data})
    failed = 0
    for i in range(0, len(records), 500):
        failed += firehose.put_record_batch(DeliveryStreamName=stream,
                                            Records=records[i:i + 500])["FailedPutCount"]
    if failed:
        raise RuntimeError(f"{failed} records failed to reach Firehose")  # Lambda/Kinesis will retry
    return {"forwarded": len(records)}
```

### Example 7 — Tests

The handler logic is tested **in-process** (no container needed) by loading `handler.py` against the emulator. Deploy/update/trigger calls are tested through the API, and a real `invoke` runs only on Floci.

`tests/test_04_compute_and_catalog.py`

```python
import json
import os
from src.common.aws import arn, client, name
from src.dynamodb_ops import items, tables
from src.iam_ops import roles
from src.kinesis_ops import streams
from src.lambda_ops import functions, mappings, packaging
from src.sqs_ops import queues

MOTO = os.getenv("EMULATOR") == "moto"  # moto cannot run Lambda containers or real SQL


def test_ingest_handler_logic_in_process(monkeypatch):
    """Run the Lambda code in-process against the emulator (no container needed)."""
    t = name("events")
    tables.create_events_table(t)
    s = name("events")
    streams.create(s, 1)
    monkeypatch.setenv("TABLE_NAME", t)
    monkeypatch.setenv("STREAM_NAME", s)
    monkeypatch.setenv("AWS_ENDPOINT_URL", os.environ["AWS_ENDPOINT_URL"])
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")  # inside real Lambda these come from the role
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    import importlib.util
    spec = importlib.util.spec_from_file_location("ingest_handler", "lambda_src/ingest/handler.py")
    h = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(h)

    s3_event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "raw/o.csv"}},
                             "eventName": "ObjectCreated:Put"}]}
    sqs_event = {"Records": [
        {"eventSource": "aws:sqs", "messageId": "m1", "body": json.dumps(s3_event)},
        {"eventSource": "aws:sqs", "messageId": "m2", "body": json.dumps({"Event": "s3:TestEvent"})},
        {"eventSource": "aws:sqs", "messageId": "m3", "body": json.dumps({"fail": True})},
    ]}

    class Ctx:
        aws_request_id = "req-1"

    out = h.lambda_handler(sqs_event, Ctx())
    assert out == {"batchItemFailures": [{"itemIdentifier": "m3"}]}
    logged = items.events_for_run(t, "req-1")
    assert sorted(i["status"] for i in logged) == ["FAILED", "OK", "OK"]
    assert h.lambda_handler({"job": "j1"}, Ctx())["statusCode"] == 200


def test_lambda_package_and_deploy():
    z = packaging.zip_dir("lambda_src/ingest")
    assert z[:2] == b"PK"
    role = roles.create_all()["lambda-role"]
    q = queues.create_with_dlq(name("jobs"))
    arn_fn = functions.deploy(name("ingest"), "lambda_src/ingest", role,
                              env={"TABLE_NAME": name("events")}, dlq_arn=q["dlq_arn"])
    assert arn_fn.endswith(name("ingest"))
    functions.deploy(name("ingest"), "lambda_src/ingest", role)  # update path
    uuid = mappings.sqs_trigger(name("ingest"), q["main_arn"])
    assert mappings.list_mappings(name("ingest"))
    mappings.delete_mapping(uuid)
    if not MOTO:
        out = functions.invoke(name("ingest"), {"job": "smoke"})
        assert out["statusCode"] == 200
```

### Example 8 — Terraform

`terraform/lambda.tf`

```hcl
data "archive_file" "ingest" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_src/ingest"
  output_path = "${path.module}/build/ingest.zip"
}

data "archive_file" "glue_runner" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_src/glue_runner"
  output_path = "${path.module}/build/glue_runner.zip"
}

data "archive_file" "kinesis_to_firehose" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_src/kinesis_to_firehose"
  output_path = "${path.module}/build/kinesis_to_firehose.zip"
}

resource "aws_lambda_function" "ingest" {
  function_name    = "${var.prefix}-ingest"
  role             = aws_iam_role.this["lambda"].arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.ingest.output_path
  source_code_hash = data.archive_file.ingest.output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      TABLE_NAME  = aws_dynamodb_table.events.name
      STREAM_NAME = aws_kinesis_stream.events.name
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }
  tags = local.tags
}

resource "aws_lambda_function" "glue_runner" {
  function_name    = "${var.prefix}-glue-runner"
  role             = aws_iam_role.this["lambda"].arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.glue_runner.output_path
  source_code_hash = data.archive_file.glue_runner.output_base64sha256
  timeout          = 30
  tags             = local.tags
}

resource "aws_lambda_function" "kinesis_to_firehose" {
  function_name    = "${var.prefix}-kinesis-to-firehose"
  role             = aws_iam_role.this["lambda"].arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.kinesis_to_firehose.output_path
  source_code_hash = data.archive_file.kinesis_to_firehose.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      FIREHOSE_STREAM = aws_kinesis_firehose_delivery_stream.to_s3.name
    }
  }
  tags = local.tags
}

resource "aws_lambda_event_source_mapping" "from_queue" {
  event_source_arn        = aws_sqs_queue.main.arn
  function_name           = aws_lambda_function.ingest.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
}

resource "aws_lambda_event_source_mapping" "from_stream" {
  event_source_arn               = aws_kinesis_stream.events.arn
  function_name                  = aws_lambda_function.kinesis_to_firehose.arn
  starting_position              = "TRIM_HORIZON"
  batch_size                     = 100
  maximum_retry_attempts         = 2
  bisect_batch_on_function_error = true
}
```

---

## Reference

Project files:

```
lambda_src/ingest/handler.py
lambda_src/glue_runner/handler.py
lambda_src/kinesis_to_firehose/handler.py
src/lambda_ops/{packaging,functions,mappings}.py
terraform/lambda.tf
```

Related: [14 SQS + DLQ](14-sqs-dlq.md), [13 DynamoDB](13-dynamodb.md), [11 Kinesis](11-kinesis-data-streams.md), [12 Firehose](12-kinesis-firehose.md), [03 Glue](03-glue.md).
