"""Bucket event notifications. The destination must already exist (Floci checks this)."""
from src.common.aws import client

s3 = client("s3")


def _filter(prefix: str, suffix: str) -> dict:
    rules = []
    if prefix:
        rules.append({"Name": "prefix", "Value": prefix})
    if suffix:
        rules.append({"Name": "suffix", "Value": suffix})
    return {"Key": {"FilterRules": rules}} if rules else {}


def notify_sqs(bucket: str, queue_arn: str, prefix: str = "raw/", suffix: str = ".csv") -> None:
    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={"QueueConfigurations": [{
            "QueueArn": queue_arn,
            "Events": ["s3:ObjectCreated:*"],
            "Filter": _filter(prefix, suffix),
        }]},
    )


def notify_lambda(bucket: str, function_arn: str, prefix: str = "raw/", suffix: str = "") -> None:
    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={"LambdaFunctionConfigurations": [{
            "LambdaFunctionArn": function_arn,
            "Events": ["s3:ObjectCreated:*"],
            "Filter": _filter(prefix, suffix),
        }]},
    )


def clear(bucket: str) -> None:
    s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration={})
