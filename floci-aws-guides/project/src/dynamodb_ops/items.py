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
