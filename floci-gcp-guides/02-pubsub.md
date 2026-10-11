# 02 — Pub/Sub

> [Index](README.md) · [← 01 — Cloud Storage](01-cloud-storage.md) · [03 — BigQuery →](03-bigquery.md)

---

## 1. Documentation

### Overview

Pub/Sub is a managed messaging service. Publishers send messages to a **topic**; each **subscription** attached to the topic receives a copy. It covers what SNS, SQS, and Kinesis do on AWS.

### At a glance

| Item | Value |
|------|-------|
| Local emulator | gcloud pubsub emulator |
| AWS equivalent | SNS / SQS / Kinesis |
| Compose service | `pubsub` |
| Host port(s) | `8085` |
| Environment variable | `PUBSUB_EMULATOR_HOST` |
| Python package | `google-cloud-pubsub` |
| Docker socket | not required |

### How it behaves on Floci

- Floci itself emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service, all wired in one `project/docker-compose.yml`, with the same "run it locally, no cloud bill" workflow as the AWS guides.
- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat it as dev/test only.
- Terraform examples target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.

---

## 2. Basic Usage

> Prerequisite: Floci-style local setup from [guide 00](00-setup-floci.md). Run commands from `project/`.

```bash
docker compose up -d pubsub
source .env.local            # sets PUBSUB_EMULATOR_HOST
```
Flow: create topic → create subscription → publish → pull → **ack**. Unacked messages are redelivered.

---

## 3. Examples

### Example 1 — Core flow

```python
from google.cloud import pubsub_v1

P = "demo-project"
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

topic = publisher.topic_path(P, "orders")
sub = subscriber.subscription_path(P, "orders-sub")

publisher.create_topic(request={"name": topic})
subscriber.create_subscription(request={"name": sub, "topic": topic, "ack_deadline_seconds": 30})

# publish (with attributes)
fut = publisher.publish(topic, b'{"order_id": 1}', source="shop")
print("published", fut.result())

# pull + ack
resp = subscriber.pull(request={"subscription": sub, "max_messages": 10}, timeout=10)
for m in resp.received_messages:
    print(m.message.data, dict(m.message.attributes))
if resp.received_messages:
    subscriber.acknowledge(request={"subscription": sub, "ack_ids": [m.ack_id for m in resp.received_messages]})
```
### Example 2 — Dead-letter topic

```python
dlq = publisher.topic_path(P, "orders-dlq")
publisher.create_topic(request={"name": dlq})
subscriber.create_subscription(request={
    "name": subscriber.subscription_path(P, "orders-sub2"),
    "topic": topic,
    "dead_letter_policy": {"dead_letter_topic": dlq, "max_delivery_attempts": 5},
})
```
### Example 3 — Terraform (real GCP)

```hcl
resource "google_pubsub_topic" "orders" { name = "orders" }
resource "google_pubsub_subscription" "orders" {
  name  = "orders-sub"
  topic = google_pubsub_topic.orders.id
  ack_deadline_seconds = 30
}
```

---

## Reference

Project files (shared by every GCP guide):

```
docker-compose.yml   (service: pubsub)
.env.local
requirements.txt
```

Related: [06 SNS](../floci-aws-guides/06-sns.md), [14 SQS + DLQ](../floci-aws-guides/14-sqs-dlq.md), [11 Kinesis Data Streams](../floci-aws-guides/11-kinesis-data-streams.md). Projects that use this service are listed in the [README](README.md#projects).
