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
