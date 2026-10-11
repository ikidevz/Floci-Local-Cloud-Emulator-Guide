import json

from src.common.aws import client
from src.sqs_ops.queues import allow_senders, queue_arn

sns = client("sns")


def subscribe_sqs(topic_arn: str, queue_url: str, filter_policy: dict | None = None,
                  raw: bool = True) -> str:
    # The queue must allow SNS to send to it, or messages are silently dropped.
    allow_senders(queue_url, "sns.amazonaws.com", topic_arn)
    attrs = {"RawMessageDelivery": str(raw).lower()}
    if filter_policy:
        attrs["FilterPolicy"] = json.dumps(filter_policy)
    return sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn(queue_url),
                         Attributes=attrs, ReturnSubscriptionArn=True)["SubscriptionArn"]


def subscribe_lambda(topic_arn: str, fn_arn: str) -> str:
    """Also needs a Lambda resource policy for sns.amazonaws.com (lambda_ops.functions.allow_service)."""
    return sns.subscribe(TopicArn=topic_arn, Protocol="lambda", Endpoint=fn_arn,
                         ReturnSubscriptionArn=True)["SubscriptionArn"]


def list_subs(topic_arn: str) -> list[dict]:
    return [{"protocol": s["Protocol"], "endpoint": s["Endpoint"]}
            for p in sns.get_paginator("list_subscriptions_by_topic").paginate(TopicArn=topic_arn)
            for s in p["Subscriptions"]]
