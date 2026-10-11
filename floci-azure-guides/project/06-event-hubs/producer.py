"""Producer side: partition KEY = vehicle_id, so every event of one vehicle lands on one partition IN ORDER.
That ordering is what makes per-vehicle state (debouncing, escalation, de-dup) possible downstream."""
import json
from collections import OrderedDict

import hub


def plan_batches(events, max_batch=100):
    """events: [(partition_key, body_str)] in send order -> [(partition_key, [bodies])] with each key's order preserved
    and no batch larger than max_batch. Pure, so the ordering guarantee is unit-testable without a broker."""
    by_key = OrderedDict()
    for key, body in events:
        by_key.setdefault(key, []).append(body)
    return [(k, bodies[i:i + max_batch]) for k, bodies in by_key.items() for i in range(0, len(bodies), max_batch)]


def key_of(raw):
    """Partition key for a raw event. Unparseable events still need one, or they could not be sent (they are dead-lettered downstream)."""
    try:
        return json.loads(raw)["vehicle_id"]
    except Exception:  # noqa: BLE001
        return "malformed"


def send(events, conn=None, hub_name=None):
    """events: [(key, body)]. One send per planned batch; same key -> same partition."""
    from azure.eventhub import EventData, EventHubProducerClient
    sent = 0
    with EventHubProducerClient.from_connection_string(conn or hub.conn_str(), eventhub_name=hub_name or hub.HUB) as p:
        for key, bodies in plan_batches(events):
            b = p.create_batch(partition_key=key)
            for body in bodies:
                b.add(EventData(body))
            p.send_batch(b)
            sent += len(bodies)
    return sent
