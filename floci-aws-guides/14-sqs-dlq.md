# 14 — SQS + Dead Letter Queue

> [Index](README.md) · [← 13 — DynamoDB](13-dynamodb.md) · [15 — End-to-End Pipeline →](15-end-to-end-pipeline.md)

A DLQ catches messages that failed too many times, so one poison message cannot block a queue.

---

## 1. Documentation

### Overview

```
S3 / producer ──► main queue ──► consumer
                     │ (fails maxReceiveCount times)
                     ▼
                dead-letter queue ──► alarm / inspect / redrive
```

- The **main queue** has a `RedrivePolicy` pointing at the **dead-letter queue** with a `maxReceiveCount`.
- A consumer **deletes** a message on success. On failure it leaves the message, which reappears after the visibility timeout. After `maxReceiveCount` receives SQS moves it to the DLQ.
- From the DLQ you can alarm, inspect, and **redrive** messages back to the main queue.

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `sqs` |
| Terraform endpoint | `sqs` |
| Needs first | nothing (S3 and SNS senders need a queue policy) |
| Docker socket | not required |

### How it behaves on Floci

- Floci implements `StartMessageMoveTask` and `ListMessageMoveTasks` (managed DLQ redrive), plus a non-destructive inspection endpoint: `GET /_aws/sqs/messages?QueueUrl=<url>`.
- `QueueUrl` uses the host from `FLOCI_HOSTNAME` (see [00](00-setup-floci.md)).
- `create_queue` with **different attributes** on an existing name raises `QueueNameExists`; `ensure_queue` handles re-runs by updating attributes.

### Key concepts

- **Visibility timeout**: how long a received message is hidden from other consumers.
- **`maxReceiveCount`**: how many times a message may be received before SQS moves it to the DLQ.
- **`RedriveAllowPolicy`**: restricts which source queues may use a DLQ (`byQueue`).
- **Managed redrive** (`redrive_api`, `StartMessageMoveTask`) vs **manual redrive** (`redrive_manual`, works anywhere).

### Gotchas

- `maxReceiveCount` counts **receives**, not failures.
- DLQ type must match the source (standard → standard).
- Keep DLQ retention **longer** than the main queue (retention counts from original enqueue).
- `VisibilityTimeout` must be ≥ the consumer's processing time (and ≥ the Lambda timeout for event source mappings).
- Use `ReportBatchItemFailures` with Lambda triggers.

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)). Floci puts the `FLOCI_HOSTNAME` host (`floci`) in queue URLs, so make sure it resolves from your machine.

```python
import json
import boto3

sqs = boto3.client("sqs")

def arn_of(url: str) -> str:
    return sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

# 1. Dead-letter queue, then the main queue that redrives into it after 3 receives
dlq_url = sqs.create_queue(QueueName="demo-jobs-dlq")["QueueUrl"]
url = sqs.create_queue(QueueName="demo-jobs", Attributes={
    "VisibilityTimeout": "30",
    "RedrivePolicy": json.dumps({"deadLetterTargetArn": arn_of(dlq_url), "maxReceiveCount": 3}),
})["QueueUrl"]

# 2. Send
sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"id": 1}))

# 3. Receive, process, delete. If you do NOT delete, the message reappears after the visibility timeout.
for m in sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", []):
    print(json.loads(m["Body"]))                  # {'id': 1}
    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
```

---

## 3. Examples

### Example 1 — Queue helpers: queue with DLQ, sender policy, URLs, cleanup

`create_with_dlq` builds `<base>-dlq` and `<base>` with a redrive policy and a `byQueue` redrive-allow policy.

`src/sqs_ops/queues.py`

```python
import json

from src.common.aws import client

sqs = client("sqs")


def queue_arn(url: str) -> str:
    return sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]


def ensure_queue(queue_name: str, attributes: dict[str, str]) -> str:
    """Create the queue, or update the attributes of an existing one (create_queue with different
    attributes raises QueueNameExists, so a plain re-run would fail)."""
    try:
        return sqs.create_queue(QueueName=queue_name, Attributes=attributes)["QueueUrl"]
    except sqs.exceptions.QueueNameExists:
        url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        sqs.set_queue_attributes(QueueUrl=url, Attributes=attributes)
        return url


def create_with_dlq(base: str, max_receive: int = 3, visibility_s: int = 30,
                    retention_days: int = 4) -> dict[str, str]:
    """Create <base>-dlq and <base> with a RedrivePolicy pointing at it."""
    dlq_url = ensure_queue(f"{base}-dlq", {"MessageRetentionPeriod": str(14 * 86400)})  # keep failures longer
    dlq_arn = queue_arn(dlq_url)

    main_url = ensure_queue(base, {
        "VisibilityTimeout": str(visibility_s),
        "MessageRetentionPeriod": str(retention_days * 86400),
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": max_receive}),
    })
    main_arn = queue_arn(main_url)

    # Only the main queue may use this DLQ
    sqs.set_queue_attributes(QueueUrl=dlq_url, Attributes={
        "RedriveAllowPolicy": json.dumps({"redrivePermission": "byQueue",
                                          "sourceQueueArns": [main_arn]})})
    return {"main_url": main_url, "main_arn": main_arn, "dlq_url": dlq_url, "dlq_arn": dlq_arn}


def allow_senders(queue_url: str, principal_service: str, source_arn: str) -> None:
    """Resource policy so s3.amazonaws.com / sns.amazonaws.com can send to the queue."""
    arn = queue_arn(queue_url)
    policy = {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": principal_service},
        "Action": "sqs:SendMessage", "Resource": arn,
        "Condition": {"ArnLike": {"aws:SourceArn": source_arn}}}]}
    sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"Policy": json.dumps(policy)})


def get_url(queue_name: str) -> str:
    return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]


def delete_pair(base: str) -> None:
    for q in (base, f"{base}-dlq"):
        try:
            sqs.delete_queue(QueueUrl=get_url(q))
        except sqs.exceptions.QueueDoesNotExist:
            pass
```

### Example 2 — Producer: single and batched sends

`src/sqs_ops/producer.py`

```python
import json
import uuid

from src.common.aws import client

sqs = client("sqs")


def send(url: str, body: dict, attrs: dict[str, str] | None = None) -> str:
    kw = dict(QueueUrl=url, MessageBody=json.dumps(body))
    if attrs:
        kw["MessageAttributes"] = {k: {"DataType": "String", "StringValue": v} for k, v in attrs.items()}
    return sqs.send_message(**kw)["MessageId"]


def send_batch(url: str, bodies: list[dict]) -> int:
    """10 messages per API call. Returns how many failed."""
    failed = 0
    for i in range(0, len(bodies), 10):
        entries = [{"Id": uuid.uuid4().hex[:8], "MessageBody": json.dumps(b)} for b in bodies[i:i + 10]]
        failed += len(sqs.send_message_batch(QueueUrl=url, Entries=entries).get("Failed", []))
    return failed
```

### Example 3 — Consumer: delete on success, leave on failure

`src/sqs_ops/consumer.py`

```python
import json
from typing import Callable

from src.common.aws import client

sqs = client("sqs")


def poll_once(url: str, handler: Callable[[dict], None], max_msgs: int = 10, wait_s: int = 1) -> dict:
    """
    Success -> delete the message.
    Failure -> leave it. It reappears after the visibility timeout, and after
               maxReceiveCount receives SQS moves it to the DLQ.
    """
    resp = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=max_msgs, WaitTimeSeconds=wait_s,
                               AttributeNames=["ApproximateReceiveCount"], MessageAttributeNames=["All"])
    ok = bad = 0
    for m in resp.get("Messages", []):
        try:
            handler(json.loads(m["Body"]))
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
            ok += 1
        except Exception as exc:  # noqa: BLE001
            bad += 1
            print(f"handler failed (receive #{m['Attributes']['ApproximateReceiveCount']}): {exc}")
    return {"ok": ok, "failed": bad}
```

### Example 4 — DLQ tools: depth, peek, manual and managed redrive

`src/sqs_ops/dlq.py`

```python
import json
import time

from src.common.aws import client
from src.sqs_ops.queues import queue_arn

sqs = client("sqs")


def depth(url: str) -> int:
    a = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
        "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    return int(a["ApproximateNumberOfMessages"]) + int(a["ApproximateNumberOfMessagesNotVisible"])


def peek(dlq_url: str, n: int = 10) -> list[dict]:
    """Read failed messages without deleting them (they become visible again after 2s)."""
    msgs = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=n, VisibilityTimeout=2,
                               WaitTimeSeconds=1).get("Messages", [])
    return [json.loads(m["Body"]) for m in msgs]


def redrive_manual(dlq_url: str, main_url: str) -> int:
    """Portable redrive: copy DLQ -> main, then delete from the DLQ."""
    moved = 0
    while True:
        msgs = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10,
                                   WaitTimeSeconds=1).get("Messages", [])
        if not msgs:
            return moved
        for m in msgs:
            sqs.send_message(QueueUrl=main_url, MessageBody=m["Body"])
            sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=m["ReceiptHandle"])
            moved += 1


def redrive_api(dlq_url: str, main_url: str, wait: bool = True, timeout: int = 30) -> str:
    """Managed redrive via StartMessageMoveTask (Floci implements it)."""
    handle = sqs.start_message_move_task(SourceArn=queue_arn(dlq_url),
                                         DestinationArn=queue_arn(main_url))["TaskHandle"]
    if wait:
        deadline = time.time() + timeout
        while time.time() < deadline:
            tasks = sqs.list_message_move_tasks(SourceArn=queue_arn(dlq_url))["Results"]
            if tasks and tasks[0]["Status"] in ("COMPLETED", "FAILED", "CANCELLED"):
                return tasks[0]["Status"]
            time.sleep(1)
    return handle
```

### Example 5 — Test: prove the DLQ works

`tests/test_02_sqs_sns.py`

```python
import time
from src.common.aws import client, name
from src.sqs_ops import consumer, dlq, producer, queues


def test_dlq_flow():
    q = queues.create_with_dlq(name("jobs"), max_receive=2, visibility_s=1)
    producer.send(q["main_url"], {"id": 1, "ok": True})
    producer.send(q["main_url"], {"id": 2, "ok": False})  # poison message
    assert producer.send_batch(q["main_url"], [{"id": 3, "ok": True}]) == 0

    def handle(msg):
        if not msg["ok"]:
            raise ValueError("cannot process")

    for _ in range(6):  # more rounds than maxReceiveCount
        consumer.poll_once(q["main_url"], handle, wait_s=1)
        time.sleep(1.2)  # let the visibility timeout expire
    assert dlq.depth(q["dlq_url"]) == 1
    assert dlq.peek(q["dlq_url"])[0]["id"] == 2

    time.sleep(2.5)
    assert dlq.redrive_manual(q["dlq_url"], q["main_url"]) == 1
```

### Example 6 — Terraform

`terraform/sqs.tf`

```hcl
resource "aws_sqs_queue" "dlq" {
  name                      = "${var.prefix}-jobs-dlq"
  message_retention_seconds = 1209600 # 14 days
  tags                      = local.tags
}

resource "aws_sqs_queue" "main" {
  name                       = "${var.prefix}-jobs"
  visibility_timeout_seconds = 60 # keep >= the Lambda timeout
  message_retention_seconds  = 345600
  tags                       = local.tags
}

resource "aws_sqs_queue_redrive_policy" "main" {
  queue_url = aws_sqs_queue.main.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main.arn]
  })
}

resource "aws_sqs_queue_policy" "main" {
  queue_url = aws_sqs_queue.main.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.main.arn
      Condition = { ArnLike = { "aws:SourceArn" = aws_s3_bucket.this["raw"].arn } }
    }]
  })
}
```

---

## Reference

Project files:

```
src/sqs_ops/{queues,producer,consumer,dlq}.py
terraform/sqs.tf
tests/test_02_sqs_sns.py
```

Related: [06 SNS](06-sns.md) (`allow_senders`), [02 Lambda](02-lambda.md) (SQS trigger), [01 S3](01-s3.md) (notifications), [08 CloudWatch](08-cloudwatch.md) (DLQ alarm).
