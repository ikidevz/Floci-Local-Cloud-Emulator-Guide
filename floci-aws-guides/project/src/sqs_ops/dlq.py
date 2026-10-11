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
