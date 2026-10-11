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
