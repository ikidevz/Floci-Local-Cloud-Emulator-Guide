"""Message builders for the two routing patterns. Two rules make a topic safe under at-least-once delivery:
  * message_id is DETERMINISTIC (a replayed batch re-sends the SAME id, so duplicate detection can absorb it)
  * everything a subscription filters on travels in application_properties (that is what SQL filters see)."""
import json

from azure.servicebus import ServiceBusMessage


def alert_message(alert: dict) -> ServiceBusMessage:
    """Streaming pipeline -> topic fleet-alerts. alert_id = hash(vehicle, rule, trigger time)."""
    return ServiceBusMessage(json.dumps(alert), message_id=alert["alert_id"], subject=alert["rule"],
                             application_properties={"severity": alert["severity"], "rule": alert["rule"], "vehicle_id": alert["vehicle_id"]})


def result_message(result: dict) -> ServiceBusMessage:
    """DQ gate -> topic dq-results. message_id = '<file name>:<content hash prefix>'."""
    return ServiceBusMessage(json.dumps(result), message_id=result["message_id"], subject=result["route"],
                             application_properties={"route": result["route"], "severity": result["severity"]})
