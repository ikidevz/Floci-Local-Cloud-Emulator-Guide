"""Event Hubs connection. UseDevelopmentEmulator=true => plain AMQP (no TLS) on :5672; the key is ignored by the emulator."""
import os

HUB = os.environ.get("EVENTHUB_NAME", "telemetry")
CONSUMER_GROUP = os.environ.get("EVENTHUB_CONSUMER_GROUP", "$Default")


def conn_str():
    return os.environ.get("EVENTHUB_CONNECTION_STRING",
                          "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=devkey;UseDevelopmentEmulator=true;")
