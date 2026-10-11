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
