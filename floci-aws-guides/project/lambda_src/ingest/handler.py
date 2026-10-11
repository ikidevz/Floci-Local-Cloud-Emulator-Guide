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
