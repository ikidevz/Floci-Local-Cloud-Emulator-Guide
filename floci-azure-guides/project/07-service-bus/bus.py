"""Publish to a topic / drain a subscription. UseDevelopmentEmulator=true => plain AMQP on :5673, key ignored."""
import os

from azure.servicebus import ServiceBusClient


def conn_str():
    return os.environ.get("SERVICEBUS_CONNECTION_STRING",
                          "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=devkey;UseDevelopmentEmulator=true;")


def publish(topic, messages, conn=None):
    with ServiceBusClient.from_connection_string(conn or conn_str()) as c, c.get_topic_sender(topic) as s:
        s.send_messages(list(messages))


def drain(topic, subscription, max_wait=5, conn=None):
    """Receive and COMPLETE everything currently on a subscription -> [(message_id, application_properties)]."""
    out = []
    with ServiceBusClient.from_connection_string(conn or conn_str()) as c, c.get_subscription_receiver(topic, subscription, max_wait_time=max_wait) as r:
        for m in r:
            out.append((m.message_id, {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in (m.application_properties or {}).items()}))
            r.complete_message(m)
    return out
