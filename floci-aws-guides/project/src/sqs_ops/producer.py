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
