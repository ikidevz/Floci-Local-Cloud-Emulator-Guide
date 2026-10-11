"""Floci AZ can only invoke Functions over HTTP and has no Event Hub TRIGGER, so this small bridge plays the trigger:
it reads batches from the hub and POSTs them to the function. Real Azure: delete the bridge, use the Event Hub trigger.

Contract: advance the checkpoint ONLY after the function answered 200. Caveat that bites: update_checkpoint() is a NO-OP
(warning only) unless the consumer has a checkpoint store. Set CHECKPOINT_CONTAINER to use a Blob-backed one; without it the
bridge starts from the beginning of the hub every run - fine ONLY because the processor is idempotent (at-least-once)."""
import json
import os
import time
import urllib.error
import urllib.request

import hub

FUNCTION_URL = os.environ.get("FUNCTION_URL", "http://localhost:4577/devstoreaccount1-functions/api/coldchain-fn/process_batch")


def post_events(bodies, url=None, timeout=120):
    req = urllib.request.Request(url or FUNCTION_URL, method="POST", data=json.dumps({"events": bodies}).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status == 200
    except (urllib.error.URLError, OSError):
        return False                                                  # HTTPError (500) is a URLError: non-200 == "not processed"


def make_handler(post, attempts=3, sleep=time.sleep, log=print):
    """-> on_event_batch(partition_context, events). Checkpoint only on success; retry a few times first."""
    def on_event_batch(pc, events):
        if not events:
            return
        bodies = [e.body_as_str() for e in events]
        for i in range(attempts):
            if post(bodies):
                pc.update_checkpoint(events[-1])                      # pass the event: batch mode has no implicit 'last event' guarantee
                return
            sleep(2 ** i)
        log(f"partition {pc.partition_id}: {len(events)} events NOT processed; checkpoint NOT advanced (they replay on restart)")
    return on_event_batch


def run(conn=None, post=post_events, stop_after=None):
    from azure.eventhub import EventHubConsumerClient
    store = None
    if os.environ.get("CHECKPOINT_CONTAINER"):
        from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
        store = BlobCheckpointStore.from_connection_string(os.environ["STORAGE_CONNECTION_STRING"], os.environ["CHECKPOINT_CONTAINER"])
    c = EventHubConsumerClient.from_connection_string(conn or hub.conn_str(), consumer_group=hub.CONSUMER_GROUP, eventhub_name=hub.HUB, checkpoint_store=store)
    if stop_after:
        import threading
        threading.Timer(stop_after, c.close).start()
    with c:
        c.receive_batch(on_event_batch=make_handler(post), max_batch_size=100, max_wait_time=5, starting_position="-1")


if __name__ == "__main__":
    run()
