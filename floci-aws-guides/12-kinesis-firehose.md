# 12 — Kinesis Data Firehose

> [Index](README.md) · [← 11 — Kinesis Data Streams](11-kinesis-data-streams.md) · [13 — DynamoDB →](13-dynamodb.md)

Buffers records and writes them to S3.

---

## 1. Documentation

### Overview

- A **delivery stream** buffers incoming records and flushes them to a destination, here S3.
- This project uses **DirectPut**: producers call `put_record` / `put_record_batch`. A small Lambda (`kinesis_to_firehose`, [guide 02](02-lambda.md)) bridges a Kinesis stream into it.

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `firehose` |
| Terraform endpoint | `firehose` |
| Needs first | an IAM role ([07](07-iam.md)) and a destination bucket ([01](01-s3.md)) |
| Docker socket | not required (Parquet conversion needs the `floci-duck` sidecar, not used here) |

### How it behaves on Floci

- **Buffering**: Floci flushes when `SizeInMBs` is reached or `IntervalInSeconds` has passed since the first buffered record. A background flusher checks every 10 s, so with `IntervalInSeconds=60` expect objects after roughly 60–70 s.
- For fast local feedback set `FLOCI_SERVICES_FIREHOSE_FLUSH_RECORD_COUNT=1` (an emulator-only trigger; off by default so timing matches AWS).
- Records are concatenated as bytes, so **append `\n`** yourself.
- Floci's Firehose docs do not document a Kinesis-stream source, so this project uses **DirectPut** and a Lambda bridge. That also works on real AWS.
- Format conversion to Parquet needs the `floci-duck` sidecar (not used here).

### Gotchas

- `ErrorOutputPrefix` is needed whenever the prefix uses expressions.
- Delete a stream and its buffered records are discarded without flushing.
- `buffering_interval` below 60 s is rejected on real AWS.

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

### Create the delivery stream and send records

```python
import json, time
import boto3
from botocore.config import Config

s3 = boto3.client("s3", config=Config(s3={"addressing_style": "path"}))
iam, fh = boto3.client("iam"), boto3.client("firehose")

s3.create_bucket(Bucket="demo-curated")
trust = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "firehose.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
role_arn = iam.create_role(RoleName="demo-firehose-role",
                           AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]

fh.create_delivery_stream(
    DeliveryStreamName="demo-to-s3", DeliveryStreamType="DirectPut",
    ExtendedS3DestinationConfiguration={
        "RoleARN": role_arn, "BucketARN": "arn:aws:s3:::demo-curated", "Prefix": "events/",
        "ErrorOutputPrefix": "events/errors/!{firehose:error-output-type}/",
        "BufferingHints": {"IntervalInSeconds": 60, "SizeInMBs": 1},
        "CompressionFormat": "UNCOMPRESSED"})

for _ in range(30):                              # wait until ACTIVE
    desc = fh.describe_delivery_stream(DeliveryStreamName="demo-to-s3")["DeliveryStreamDescription"]
    if desc["DeliveryStreamStatus"] == "ACTIVE":
        break
    time.sleep(1)

# Append "\n" so the S3 object is newline-delimited JSON (Firehose concatenates raw bytes)
resp = fh.put_record_batch(DeliveryStreamName="demo-to-s3",
                           Records=[{"Data": (json.dumps({"id": i}) + "\n").encode()} for i in range(10)])
print("failed:", resp["FailedPutCount"])         # failed: 0
```

### Wait for the flush

```python
# (needs Floci: the buffer flushes after ~60-70 s, or set FLOCI_SERVICES_FIREHOSE_FLUSH_RECORD_COUNT=1)
for _ in range(50):
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="demo-curated", Prefix="events/").get("Contents", [])]
    if keys:
        break
    time.sleep(3)
print(keys)
```

---

## 3. Examples

### Example 1 — Delivery stream to S3 (create, reuse, delete)

`src/firehose_ops/streams.py`

```python
from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

fh = client("firehose")


def create_to_s3(stream: str, role_arn: str, bucket: str, prefix: str = "firehose/",
                 buffer_s: int = 60, buffer_mb: int = 1, gzip: bool = False) -> str:
    """DirectPut delivery stream -> S3. Returns the stream ARN."""
    cfg = {
        "RoleARN": role_arn,
        "BucketARN": f"arn:aws:s3:::{bucket}",
        "Prefix": prefix,
        "ErrorOutputPrefix": prefix + "errors/!{firehose:error-output-type}/",
        "BufferingHints": {"IntervalInSeconds": buffer_s, "SizeInMBs": buffer_mb},
        "CompressionFormat": "GZIP" if gzip else "UNCOMPRESSED",
    }
    try:
        arn = fh.create_delivery_stream(DeliveryStreamName=stream, DeliveryStreamType="DirectPut",
                                        ExtendedS3DestinationConfiguration=cfg)["DeliveryStreamARN"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
        arn = fh.describe_delivery_stream(DeliveryStreamName=stream)[
            "DeliveryStreamDescription"]["DeliveryStreamARN"]
    wait_until(lambda: fh.describe_delivery_stream(DeliveryStreamName=stream)
               ["DeliveryStreamDescription"]["DeliveryStreamStatus"], done={"ACTIVE"}, timeout=60)
    return arn


def delete(stream: str) -> None:
    fh.delete_delivery_stream(DeliveryStreamName=stream, AllowForceDelete=True)
```

### Example 2 — Producer: batched sends and wait for the flush

`src/firehose_ops/producer.py`

```python
import json
import time

from src.common.aws import client

fh = client("firehose")
s3 = client("s3")


def send(stream: str, records: list[dict]) -> int:
    """put_record_batch allows 500 records / 4 MiB. Append \\n so the output is newline-delimited JSON."""
    failed = 0
    for i in range(0, len(records), 500):
        chunk = [{"Data": (json.dumps(r) + "\n").encode()} for r in records[i:i + 500]]
        failed += fh.put_record_batch(DeliveryStreamName=stream, Records=chunk)["FailedPutCount"]
    return failed


def wait_for_objects(bucket: str, prefix: str, timeout: int = 150, interval: float = 3) -> list[str]:
    """Firehose flushes only when the buffer interval/size is hit, so poll S3."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])]
        if keys:
            return keys
        time.sleep(interval)
    raise TimeoutError("Firehose has not flushed to S3. Lower BufferingHints or set "
                       "FLOCI_SERVICES_FIREHOSE_FLUSH_RECORD_COUNT=1 for local dev.")
```

### Example 3 — Send and wait

```python
from src.common.aws import name
from src.firehose_ops import producer, streams
from src.iam_ops.roles import create_all

role = create_all()["firehose-role"]
streams.create_to_s3(name("to-s3"), role, name("curated"), prefix="events/")
print("failed:", producer.send(name("to-s3"), [{"id": i} for i in range(100)]))
print(producer.wait_for_objects(name("curated"), "events/"))
```

### Example 4 — Test

`tests/test_03_data_services.py`

```python
from src.common.aws import client, name
from src.firehose_ops import producer as fh_producer
from src.firehose_ops import streams as fh_streams
from src.iam_ops import roles
from src.s3_ops import buckets


def test_firehose_direct_put():
    b = buckets.create_pipeline_buckets()["curated"]
    role = roles.create_all()["firehose-role"]
    s = name("to-s3")
    fh_streams.create_to_s3(s, role, b, prefix="events/", buffer_s=60, buffer_mb=1)
    assert fh_producer.send(s, [{"id": i} for i in range(20)]) == 0
    assert fh_producer.wait_for_objects(b, "events/", timeout=90)
```

### Example 5 — Terraform

Kinesis → Firehose is bridged by the `kinesis_to_firehose` Lambda (`lambda.tf`).

`terraform/firehose.tf`

```hcl
# DirectPut. Kinesis -> Firehose is bridged by the kinesis_to_firehose Lambda (lambda.tf).
resource "aws_kinesis_firehose_delivery_stream" "to_s3" {
  name        = "${var.prefix}-to-s3"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.this["firehose"].arn
    bucket_arn          = aws_s3_bucket.this["curated"].arn
    prefix              = "events/"
    error_output_prefix = "events/errors/!{firehose:error-output-type}/"
    buffering_size      = 1
    buffering_interval  = 60
    compression_format  = "UNCOMPRESSED"
  }

  tags = local.tags
}
```

---

## Reference

Project files:

```
src/firehose_ops/{streams,producer}.py
terraform/firehose.tf
tests/test_03_data_services.py     # test_firehose_direct_put
```

Related: [11 Kinesis Data Streams](11-kinesis-data-streams.md), [02 Lambda](02-lambda.md) (the bridge), [01 S3](01-s3.md) (destination).
