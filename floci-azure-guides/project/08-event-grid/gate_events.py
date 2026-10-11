"""The Event Grid webhook contract, as a pure function (this is what the DQ-gate Function does with every POST).
  1. answer the SubscriptionValidationEvent handshake by echoing the validationCode
  2. process BlobCreated events for the INBOX only. Our own outputs are blobs too: without this guard the gate would trigger itself.
  3. Event Grid delivers AT LEAST ONCE: `process` must be idempotent, and an exception must become a non-2xx so Event Grid retries."""
INBOX = "/blobServices/default/containers/inbox/blobs/"
VALIDATION = "Microsoft.EventGrid.SubscriptionValidationEvent"
BLOB_CREATED = "Microsoft.Storage.BlobCreated"


def handle_events(events, process):
    if events and events[0].get("eventType") == VALIDATION:
        return {"validationResponse": events[0]["data"]["validationCode"]}
    done, ignored = [], 0
    for e in events:
        subject = e.get("subject", "")
        if e.get("eventType") == BLOB_CREATED and subject.startswith(INBOX):
            done.append(process(subject[len(INBOX):]))
        else:
            ignored += 1
    return {"processed": done, "ignored": ignored}
