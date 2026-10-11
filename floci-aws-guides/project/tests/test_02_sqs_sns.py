import time

from src.common.aws import client, name
from src.sns_ops import subscriptions, topics
from src.sqs_ops import consumer, dlq, producer, queues


def test_dlq_flow():
    q = queues.create_with_dlq(name("jobs"), max_receive=2, visibility_s=1)
    producer.send(q["main_url"], {"id": 1, "ok": True})
    producer.send(q["main_url"], {"id": 2, "ok": False})  # poison message
    assert producer.send_batch(q["main_url"], [{"id": 3, "ok": True}]) == 0

    def handle(msg):
        if not msg["ok"]:
            raise ValueError("cannot process")

    for _ in range(6):  # more rounds than maxReceiveCount
        consumer.poll_once(q["main_url"], handle, wait_s=1)
        time.sleep(1.2)  # let the visibility timeout expire
    assert dlq.depth(q["dlq_url"]) == 1
    assert dlq.peek(q["dlq_url"])[0]["id"] == 2

    time.sleep(2.5)
    assert dlq.redrive_manual(q["dlq_url"], q["main_url"]) == 1


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
