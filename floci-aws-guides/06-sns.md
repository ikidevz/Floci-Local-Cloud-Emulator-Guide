# 06 — SNS

> [Index](README.md) · [← 05 — EventBridge Scheduler](05-eventbridge-scheduler.md) · [07 — IAM →](07-iam.md)

Fan-out for alerts: Step Functions and CloudWatch alarms publish; SQS or Lambda subscribe.

---

## 1. Documentation

### Overview

- A **topic** receives messages from publishers and delivers a copy to every **subscription**.
- In this project the `alerts` topic receives Step Functions notifications and CloudWatch alarm actions; an SQS queue subscribes with a filter so it only gets errors.

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `sns` |
| Terraform endpoint | `sns` |
| Needs first | a subscriber: an SQS queue ([14](14-sqs-dlq.md)) or a Lambda ([02](02-lambda.md)) |
| Docker socket | not required |

### Key concepts

- **Subscription protocols** used here: `sqs` and `lambda`.
- **Filter policy**: JSON rules that decide which messages a subscription receives.
- **Raw message delivery**: delivers the message as sent, instead of wrapped in an SNS JSON envelope.
- **FIFO topics**: name ends in `.fifo`; `ContentBasedDeduplication` avoids duplicates.
- **Queue policy**: a resource policy on the queue that authorises a sender such as `sns.amazonaws.com`.

### Gotchas

- Without the queue policy the subscription exists but messages are silently dropped (`subscribe_sqs` sets it).
- Filter policies match **message attributes** by default, not the body.
- With `RawMessageDelivery=false` the SQS body is the SNS JSON envelope (`Message` is a string inside it).
- Verify behavior with SQS or Lambda subscriptions rather than email.
- A Lambda subscriber also needs a Lambda resource policy for `sns.amazonaws.com` (`lambda_ops.functions.allow_service`).

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

```python
import json
import boto3

sns, sqs = boto3.client("sns"), boto3.client("sqs")

# 1. Topic and a queue to receive from
topic_arn = sns.create_topic(Name="demo-alerts")["TopicArn"]
queue_url = sqs.create_queue(QueueName="demo-alerts-errors")["QueueUrl"]
queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url,
                                     AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

# 2. Allow SNS to send to the queue (required on real AWS, harmless on Floci)
policy = {"Version": "2012-10-17", "Statement": [{
    "Effect": "Allow", "Principal": {"Service": "sns.amazonaws.com"},
    "Action": "sqs:SendMessage", "Resource": queue_arn,
    "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}}}]}
sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"Policy": json.dumps(policy)})

# 3. Subscribe with a filter: only ERROR and CRITICAL get through
sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn,
              Attributes={"RawMessageDelivery": "true",
                          "FilterPolicy": json.dumps({"severity": ["ERROR", "CRITICAL"]})})

# 4. Publish with a message attribute that the filter can match
def publish(message: dict, severity: str) -> None:
    sns.publish(TopicArn=topic_arn, Message=json.dumps(message),
                MessageAttributes={"severity": {"DataType": "String", "StringValue": severity}})

publish({"msg": "disk ok"}, "INFO")        # filtered out
publish({"msg": "job failed"}, "ERROR")    # delivered

for m in sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2, MaxNumberOfMessages=10).get("Messages", []):
    print(m["Body"])                       # {"msg": "job failed"}
```

---

## 3. Examples

### Example 1 — Topic helpers (create, publish, delete)

`src/sns_ops/topics.py`

```python
import json

from src.common.aws import client

sns = client("sns")


def create_topic(topic_name: str, fifo: bool = False) -> str:
    attrs = {"FifoTopic": "true", "ContentBasedDeduplication": "true"} if fifo else {}
    return sns.create_topic(Name=topic_name + (".fifo" if fifo else ""), Attributes=attrs)["TopicArn"]


def publish(topic_arn: str, message: dict | str, subject: str | None = None,
            attributes: dict[str, str] | None = None) -> str:
    kwargs = dict(TopicArn=topic_arn, Message=message if isinstance(message, str) else json.dumps(message))
    if subject:
        kwargs["Subject"] = subject
    if attributes:
        kwargs["MessageAttributes"] = {k: {"DataType": "String", "StringValue": v}
                                       for k, v in attributes.items()}
    return sns.publish(**kwargs)["MessageId"]


def delete_topic(topic_arn: str) -> None:
    sns.delete_topic(TopicArn=topic_arn)
```

### Example 2 — Subscriptions (SQS with filter, Lambda, list)

`src/sns_ops/subscriptions.py`

```python
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
```

### Example 3 — Filtered fan-out end to end

```python
from src.common.aws import client, name
from src.sns_ops.topics import create_topic, publish
from src.sns_ops.subscriptions import subscribe_sqs

topic = create_topic(name("alerts"))
q = client("sqs").create_queue(QueueName=name("alerts-errors"))["QueueUrl"]
subscribe_sqs(topic, q, filter_policy={"severity": ["ERROR", "CRITICAL"]})
publish(topic, {"msg": "disk ok"}, attributes={"severity": "INFO"})      # filtered out
publish(topic, {"msg": "job failed"}, attributes={"severity": "ERROR"})  # delivered
print(client("sqs").receive_message(QueueUrl=q, WaitTimeSeconds=2).get("Messages"))
```

### Example 4 — Test

`tests/test_02_sqs_sns.py`

```python
from src.common.aws import client, name
from src.sns_ops import subscriptions, topics


def test_sns_fanout_with_filter():
    topic = topics.create_topic(name("alerts"))
    qurl = client("sqs").create_queue(QueueName=name("alerts-errors"))["QueueUrl"]
    subscriptions.subscribe_sqs(topic, qurl, filter_policy={"severity": ["ERROR"]})
    topics.publish(topic, {"msg": "ok"}, attributes={"severity": "INFO"})
    topics.publish(topic, {"msg": "boom"}, attributes={"severity": "ERROR"})
    msgs = client("sqs").receive_message(QueueUrl=qurl, WaitTimeSeconds=2,
                                         MaxNumberOfMessages=10).get("Messages", [])
    assert [m["Body"] for m in msgs] == ['{"msg": "boom"}']
    assert subscriptions.list_subs(topic)[0]["protocol"] == "sqs"
```

### Example 5 — Terraform

`terraform/sns.tf`

```hcl
resource "aws_sns_topic" "alerts" {
  name = "${var.prefix}-alerts"
  tags = local.tags
}

resource "aws_sqs_queue" "alerts_errors" {
  name = "${var.prefix}-alerts-errors"
}

resource "aws_sqs_queue_policy" "alerts_errors" {
  queue_url = aws_sqs_queue.alerts_errors.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.alerts_errors.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_sns_topic.alerts.arn } }
    }]
  })
}

resource "aws_sns_topic_subscription" "errors_only" {
  topic_arn            = aws_sns_topic.alerts.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.alerts_errors.arn
  raw_message_delivery = true
  filter_policy        = jsonencode({ severity = ["ERROR", "CRITICAL"] })
  depends_on           = [aws_sqs_queue_policy.alerts_errors]
}
```

---

## Reference

Project files:

```
src/sns_ops/{topics,subscriptions}.py
terraform/sns.tf
tests/test_02_sqs_sns.py
```

Related: [14 SQS + DLQ](14-sqs-dlq.md) (`allow_senders`), [08 CloudWatch](08-cloudwatch.md) (alarm actions), [04 Step Functions](04-step-functions.md) (publishes success/failure).
