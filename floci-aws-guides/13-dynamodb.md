# 13 — DynamoDB

> [Index](README.md) · [← 12 — Kinesis Data Firehose](12-kinesis-firehose.md) · [14 — SQS + Dead Letter Queue →](14-sqs-dlq.md)

Pipeline state: run log, idempotency keys and events.

---

## 1. Documentation

### Overview

- The `events` table logs every pipeline run: partition key `run_id`, sort key `ts`.
- A **global secondary index** (`status-ts-index`) finds events by status.
- **TTL** (`expires_at`) expires old entries; **streams** are enabled with `NEW_AND_OLD_IMAGES`.
- A conditional write provides **idempotency**: the first claim wins, duplicates (such as SQS redelivery) are rejected.

### At a glance

| Item | Value |
|------|-------|
| boto3 client / resource | `dynamodb` (client and resource) |
| Terraform endpoint | `dynamodb` |
| Needs first | nothing |
| Docker socket | not required |

### Key concepts

- **Key schema**: `HASH` = partition key, `RANGE` = sort key.
- **GSI**: lets you query by another attribute (here `status`).
- **Conditional writes**: `ConditionExpression` makes put/update fail if the condition is false.
- **Batch writer**: chunks of 25 plus retries of unprocessed items.
- **Transactions**: `transact_write_items` updates several items all-or-nothing.

### Gotchas

- Only key attributes (and GSI keys) go in `AttributeDefinitions`.
- The resource API rejects Python `float`; use `Decimal`.
- `Query` needs the partition key; avoid `Scan` in pipelines.
- `status` is a reserved word: use `ExpressionAttributeNames`.
- TTL deletion is lazy on AWS.

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

```python
import boto3
from boto3.dynamodb.conditions import Key

ddb = boto3.resource("dynamodb")

# 1. Create a table (partition key + sort key) and wait until it exists
table = ddb.create_table(
    TableName="demo-events", BillingMode="PAY_PER_REQUEST",
    AttributeDefinitions=[{"AttributeName": "run_id", "AttributeType": "S"},
                          {"AttributeName": "ts", "AttributeType": "S"}],
    KeySchema=[{"AttributeName": "run_id", "KeyType": "HASH"},
               {"AttributeName": "ts", "KeyType": "RANGE"}])
table.wait_until_exists()

# 2. Write and read one item
table.put_item(Item={"run_id": "run-1", "ts": "2026-10-10T08:00:00Z", "status": "STARTED"})
print(table.get_item(Key={"run_id": "run-1", "ts": "2026-10-10T08:00:00Z"})["Item"])

# 3. Query everything for one partition key
items = table.query(KeyConditionExpression=Key("run_id").eq("run-1"))["Items"]
print(len(items))                                  # 1

# 4. Delete
table.delete_item(Key={"run_id": "run-1", "ts": "2026-10-10T08:00:00Z"})
```

---

## 3. Examples

### Example 1 — Table with GSI, stream and TTL

`src/dynamodb_ops/tables.py`

```python
from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

ddb = client("dynamodb")


def create_events_table(table: str) -> str:
    """PK = run_id, SK = ts; GSI by status. Returns the table ARN."""
    try:
        ddb.create_table(
            TableName=table,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "run_id", "AttributeType": "S"},
                {"AttributeName": "ts", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "run_id", "KeyType": "HASH"},
                       {"AttributeName": "ts", "KeyType": "RANGE"}],
            GlobalSecondaryIndexes=[{
                "IndexName": "status-ts-index",
                "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"},
                              {"AttributeName": "ts", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"}}],
            StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    wait_until(lambda: ddb.describe_table(TableName=table)["Table"]["TableStatus"],
               done={"ACTIVE"}, timeout=60)
    return ddb.describe_table(TableName=table)["Table"]["TableArn"]


def enable_ttl(table: str, attribute: str = "expires_at") -> None:
    ddb.update_time_to_live(TableName=table,
                            TimeToLiveSpecification={"Enabled": True, "AttributeName": attribute})


def delete(table: str) -> None:
    ddb.delete_table(TableName=table)
```

### Example 2 — Items: put, get, query, update, batch and transaction

`src/dynamodb_ops/items.py`

```python
import time
from decimal import Decimal

from boto3.dynamodb.conditions import Attr, Key

from src.common.aws import client, resource

ddb = resource("dynamodb")


def table(name: str):
    return ddb.Table(name)


def put_event(name: str, run_id: str, ts: str, status: str, **extra) -> None:
    """The resource API rejects float; convert to Decimal."""
    item = {"run_id": run_id, "ts": ts, "status": status,
            "expires_at": int(time.time()) + 7 * 86400,
            **{k: Decimal(str(v)) if isinstance(v, float) else v for k, v in extra.items()}}
    table(name).put_item(Item=item)


def get_event(name: str, run_id: str, ts: str) -> dict | None:
    return table(name).get_item(Key={"run_id": run_id, "ts": ts}).get("Item")


def events_for_run(name: str, run_id: str) -> list[dict]:
    t, items = table(name), []
    kw = {"KeyConditionExpression": Key("run_id").eq(run_id)}
    while True:
        r = t.query(**kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            return items
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def events_by_status(name: str, status: str) -> list[dict]:
    return table(name).query(IndexName="status-ts-index",
                             KeyConditionExpression=Key("status").eq(status))["Items"]


def update_status(name: str, run_id: str, ts: str, new_status: str) -> dict:
    return table(name).update_item(
        Key={"run_id": run_id, "ts": ts},
        UpdateExpression="SET #s = :s",
        ExpressionAttributeNames={"#s": "status"},  # 'status' is a DynamoDB reserved word
        ExpressionAttributeValues={":s": new_status},
        ConditionExpression=Attr("run_id").exists(),
        ReturnValues="ALL_NEW")["Attributes"]


def batch_put(name: str, items: list[dict]) -> None:
    with table(name).batch_writer() as bw:  # chunks of 25 + retries unprocessed items
        for it in items:
            bw.put_item(Item=it)


def set_status_atomic(name: str, keys: list[tuple[str, str]], status: str) -> None:
    """All-or-nothing status update for several items (transaction)."""
    client("dynamodb").transact_write_items(TransactItems=[
        {"Update": {"TableName": name,
                    "Key": {"run_id": {"S": rid}, "ts": {"S": ts}},
                    "UpdateExpression": "SET #s = :s",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {":s": {"S": status}}}}
        for rid, ts in keys])
```

### Example 3 — Idempotent claim

`src/dynamodb_ops/idempotency.py`

```python
from botocore.exceptions import ClientError

from src.common.aws import client

ddb = client("dynamodb")


def claim(table: str, run_id: str, ts: str) -> bool:
    """True the first time a (run_id, ts) is seen; False on duplicates (e.g. SQS redelivery)."""
    try:
        ddb.put_item(TableName=table,
                     Item={"run_id": {"S": run_id}, "ts": {"S": ts}, "status": {"S": "CLAIMED"}},
                     ConditionExpression="attribute_not_exists(run_id)")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise
```

### Example 4 — Create, write and query by run and by status

```python
from src.common.aws import name
from src.dynamodb_ops import items, tables

t = name("events")
tables.create_events_table(t); tables.enable_ttl(t)
items.put_event(t, "run-1", "2026-10-10T08:00:00Z", "STARTED", rows=120, cost=1.5)
items.put_event(t, "run-1", "2026-10-10T08:05:00Z", "FAILED")
print(items.events_for_run(t, "run-1"))
print(items.events_by_status(t, "FAILED"))
```

### Example 5 — Test

Covers GSI queries, a conditional update, batch writes of 30 items, a transaction and idempotent claims.

`tests/test_03_data_services.py`

```python
import pytest
from botocore.exceptions import ClientError
from src.common.aws import client, name
from src.dynamodb_ops import idempotency, items, tables


def test_dynamodb():
    t = name("events")
    tables.create_events_table(t)
    tables.enable_ttl(t)
    items.put_event(t, "run-1", "2026-10-10T08:00:00Z", "STARTED", rows=120, cost=1.5)
    items.put_event(t, "run-1", "2026-10-10T08:05:00Z", "FAILED")
    assert len(items.events_for_run(t, "run-1")) == 2
    assert len(items.events_by_status(t, "FAILED")) == 1
    assert items.update_status(t, "run-1", "2026-10-10T08:05:00Z", "RETRIED")["status"] == "RETRIED"
    items.batch_put(t, [{"run_id": "run-2", "ts": f"t{i}", "status": "NEW"} for i in range(30)])
    assert len(items.events_for_run(t, "run-2")) == 30
    items.set_status_atomic(t, [("run-2", "t0"), ("run-2", "t1")], "DONE")
    assert items.get_event(t, "run-2", "t0")["status"] == "DONE"
    assert idempotency.claim(t, "run-3", "x") is True
    assert idempotency.claim(t, "run-3", "x") is False
    with pytest.raises(ClientError):
        items.update_status(t, "nope", "nope", "X")  # condition fails: item missing
```

### Example 6 — Terraform

`terraform/dynamodb.tf`

```hcl
resource "aws_dynamodb_table" "events" {
  name         = "${var.prefix}-events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "run_id"
  range_key    = "ts"

  attribute {
    name = "run_id"
    type = "S"
  }
  attribute {
    name = "ts"
    type = "S"
  }
  attribute {
    name = "status"
    type = "S"
  }

  global_secondary_index {
    name            = "status-ts-index"
    hash_key        = "status"
    range_key       = "ts"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"
  tags             = local.tags
}
```

---

## Reference

Project files:

```
src/dynamodb_ops/{tables,items,idempotency}.py
terraform/dynamodb.tf
tests/test_03_data_services.py     # test_dynamodb
```

Related: [02 Lambda](02-lambda.md) (`ingest` writes the run log), [14 SQS + DLQ](14-sqs-dlq.md) (redelivery makes idempotency matter).
