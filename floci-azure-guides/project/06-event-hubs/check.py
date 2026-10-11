"""python check.py   (Floci AZ running)  -> send events for 3 vehicles, read them back, prove per-key ordering and partition affinity."""
import json
import sys
import threading

import hub
import producer

VEHICLES, PER = ["V-0001", "V-0002", "V-0003"], 8


def main():
    from azure.eventhub import EventHubConsumerClient
    events = [(v, json.dumps({"vehicle_id": v, "seq": i})) for i in range(PER) for v in VEHICLES]       # interleaved on the wire
    print("sent", producer.send(events))
    seen, lock = [], threading.Lock()

    def on_event(pc, e):
        if e is None:
            return
        d = json.loads(e.body_as_str())
        with lock:
            seen.append((pc.partition_id, d["vehicle_id"], d["seq"]))
    c = EventHubConsumerClient.from_connection_string(hub.conn_str(), consumer_group=hub.CONSUMER_GROUP, eventhub_name=hub.HUB)
    threading.Timer(20, c.close).start()
    with c:
        c.receive(on_event=on_event, starting_position="-1", max_wait_time=None)
    ok = True
    for v in VEHICLES:
        mine = [(p, s) for p, vid, s in seen if vid == v]
        parts = {p for p, _ in mine}
        in_order = [s for _, s in mine][-PER:] == list(range(PER))
        print("PASS" if len(parts) == 1 and in_order else "FAIL", f"{v}: partitions={sorted(parts)} in-order={in_order} events={len(mine)}")
        ok &= len(parts) == 1 and in_order
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
