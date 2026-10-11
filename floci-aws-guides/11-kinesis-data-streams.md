# 11 — Kinesis Data Streams

> [Index](README.md) · [← 10 — Athena](10-athena.md) · [12 — Kinesis Data Firehose →](12-kinesis-firehose.md)

Real-time ingestion. Producers write by partition key; consumers read by shard, or a Lambda trigger reads for you.

---

## 1. Documentation

### Overview

- A **stream** is split into **shards**; the **partition key** of each record decides its shard.
- Consumers either poll shards directly (`get_shard_iterator` + `get_records`) or let a Lambda event source mapping do it.
- In this project `ingest` publishes events to the stream, and the `kinesis-to-firehose` Lambda forwards them to Firehose.

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `kinesis` |
| Terraform endpoint | `kinesis` |
| Needs first | nothing (a Lambda trigger needs the function, [02](02-lambda.md)) |
| Docker socket | not required |

### Key concepts

- **Partition key**: hashed to pick the shard a record is written to.
- **Shard iterator**: `TRIM_HORIZON` reads from the start of the shard, `LATEST` only new records.
- **Batch puts**: `put_records` sends up to 500 records per call.
- **Lambda trigger**: an event source mapping reads the stream for you.

### Gotchas

- The **partition key** decides the shard; skewed keys create hot shards.
- `put_records` can partially fail with HTTP 200: always check `FailedRecordCount` (`put_many` retries them).
- In Lambda events Kinesis data is base64; decode `record["kinesis"]["data"]`.
- Follow `NextShardIterator`; an old iterator expires after 5 minutes.

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

```python
import json
import boto3

kds = boto3.client("kinesis")

# 1. Create the stream and wait until it is ACTIVE
kds.create_stream(StreamName="demo-events", ShardCount=1)
kds.get_waiter("stream_exists").wait(StreamName="demo-events")

# 2. Produce: the partition key picks the shard
kds.put_record(StreamName="demo-events", PartitionKey="u1",
               Data=json.dumps({"user": "u1", "n": 1}).encode())

# 3. Consume: read the shard from the beginning
shard_id = kds.list_shards(StreamName="demo-events")["Shards"][0]["ShardId"]
iterator = kds.get_shard_iterator(StreamName="demo-events", ShardId=shard_id,
                                  ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
records = kds.get_records(ShardIterator=iterator, Limit=100)["Records"]
print([json.loads(r["Data"]) for r in records])     # [{'user': 'u1', 'n': 1}]
```

---

## 3. Examples

### Example 1 — Stream helpers (create and wait, shards, retention, delete)

`src/kinesis_ops/streams.py`

```python
from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

kds = client("kinesis")


def create(stream: str, shards: int = 2) -> str:
    """Create the stream (idempotent), wait until ACTIVE, return its ARN."""
    try:
        kds.create_stream(StreamName=stream, ShardCount=shards)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    wait_until(lambda: kds.describe_stream_summary(StreamName=stream)
               ["StreamDescriptionSummary"]["StreamStatus"], done={"ACTIVE"}, timeout=60)
    return kds.describe_stream_summary(StreamName=stream)["StreamDescriptionSummary"]["StreamARN"]


def shard_ids(stream: str) -> list[str]:
    return [s["ShardId"] for p in kds.get_paginator("list_shards").paginate(StreamName=stream)
            for s in p["Shards"]]


def set_retention(stream: str, hours: int = 48) -> None:
    kds.increase_stream_retention_period(StreamName=stream, RetentionPeriodHours=hours)


def delete(stream: str) -> None:
    kds.delete_stream(StreamName=stream, EnforceConsumerDeletion=True)
```

### Example 2 — Producer: single and batched puts with retry

`src/kinesis_ops/producer.py`

```python
import json

from src.common.aws import client

kds = client("kinesis")
MAX_BATCH = 500  # API limit per put_records call


def put(stream: str, record: dict, partition_key: str) -> dict:
    r = kds.put_record(StreamName=stream, Data=json.dumps(record).encode(), PartitionKey=partition_key)
    return {"shard": r["ShardId"], "seq": r["SequenceNumber"]}


def put_many(stream: str, records: list[dict], key_field: str) -> int:
    """Batch put; retries partial failures up to 3 times. Returns the number still failed."""
    failed_total = 0
    for i in range(0, len(records), MAX_BATCH):
        batch = [{"Data": json.dumps(r).encode(), "PartitionKey": str(r[key_field])}
                 for r in records[i:i + MAX_BATCH]]
        for _ in range(3):
            resp = kds.put_records(StreamName=stream, Records=batch)
            if resp["FailedRecordCount"] == 0:
                batch = []
                break
            batch = [b for b, res in zip(batch, resp["Records"]) if "ErrorCode" in res]
        failed_total += len(batch)
    return failed_total
```

### Example 3 — Consumer: read a shard or all shards

`src/kinesis_ops/consumer.py`

```python
import json
import time
from typing import Callable

from src.common.aws import client
from src.kinesis_ops.streams import shard_ids

kds = client("kinesis")


def read_shard(stream: str, shard_id: str, handler: Callable[[dict], None],
               from_start: bool = True, idle_stops: int = 2) -> int:
    """Read one shard until `idle_stops` consecutive empty polls. Returns records handled."""
    it = kds.get_shard_iterator(
        StreamName=stream, ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON" if from_start else "LATEST")["ShardIterator"]
    count = idle = 0
    while it and idle < idle_stops:
        resp = kds.get_records(ShardIterator=it, Limit=100)
        for rec in resp["Records"]:
            handler(json.loads(rec["Data"]))
            count += 1
        idle = idle + 1 if not resp["Records"] else 0
        it = resp.get("NextShardIterator")
        time.sleep(0.5)  # stay under 5 GetRecords calls/sec/shard
    return count


def read_all(stream: str, handler: Callable[[dict], None]) -> int:
    return sum(read_shard(stream, s, handler) for s in shard_ids(stream))
```

### Example 4 — Produce and consume

```python
from src.common.aws import name
from src.kinesis_ops import consumer, producer, streams

s = name("events")
streams.create(s, shards=2)
producer.put_many(s, [{"user": f"u{i % 5}", "n": i} for i in range(25)], key_field="user")
got = []
print(consumer.read_all(s, got.append), got[:2])
```

### Example 5 — Test

`tests/test_03_data_services.py`

```python
from src.common.aws import client, name
from src.kinesis_ops import consumer, producer, streams


def test_kinesis():
    s = name("events")
    arn = streams.create(s, shards=2)
    assert arn.endswith(s)
    assert len(streams.shard_ids(s)) == 2
    assert producer.put_many(s, [{"user": f"u{i % 5}", "n": i} for i in range(25)], "user") == 0
    got: list[dict] = []
    assert consumer.read_all(s, got.append) == 25
    assert {g["n"] for g in got} == set(range(25))
```

### Example 6 — Terraform

The Lambda trigger from the stream to the Firehose bridge is in `terraform/lambda.tf` ([guide 02](02-lambda.md)).

`terraform/kinesis.tf`

```hcl
resource "aws_kinesis_stream" "events" {
  name             = "${var.prefix}-events"
  shard_count      = 2
  retention_period = 24
  tags             = local.tags
}
```

---

## Reference

Project files:

```
src/kinesis_ops/{streams,producer,consumer}.py
terraform/kinesis.tf
tests/test_03_data_services.py     # test_kinesis
```

Related: [02 Lambda](02-lambda.md) (`ingest` publishes, `kinesis-to-firehose` consumes), [12 Firehose](12-kinesis-firehose.md).
