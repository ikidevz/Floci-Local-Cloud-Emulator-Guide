import json
import pathlib
import re
import sqlite3

import pytest
import sqlglot
from sqlglot import exp

import messages as M

ROOT = pathlib.Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "Config.json").read_text())["UserConfig"]["Namespaces"][0]
SUBS = [(t["Name"], s["Name"], s["Rules"][0]["Properties"]["SqlFilter"]["SqlExpression"]) for t in CFG["Topics"] for s in t["Subscriptions"]]


def iso_seconds(d):                                           # PT20S / PT1M / PT1H / P1D ... -> seconds
    m = re.fullmatch(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?", d)
    days, h, mi, s = (int(x or 0) for x in m.groups())
    return days * 86400 + h * 3600 + mi * 60 + s


def test_config_respects_floci_limits():                     # limits documented by Floci AZ (it silently SKIPS invalid entities)
    for t in CFG["Topics"]:
        p = t["Properties"]
        assert 20 <= iso_seconds(p["DuplicateDetectionHistoryTimeWindow"]) <= 7 * 86400 and p["RequiresDuplicateDetection"] is True
        for s in t["Subscriptions"]:
            sp = s["Properties"]
            assert iso_seconds(sp["LockDuration"]) <= 300 and 1 <= sp["MaxDeliveryCount"] <= 2000
            assert "ForwardTo" not in sp and "ForwardDeadLetteredMessagesTo" not in sp       # not emulated: Floci only logs a warning
            assert len(s["Rules"]) == 1 and s["Rules"][0]["Properties"]["FilterType"] == "Sql"


def test_filters_never_use_fields_the_emulator_rejects():     # MessageId/To/ReplyTo/ContentType filters are HTTP 400 in Floci AZ
    for _, _, expr in SUBS:
        assert not re.search(r"messageid|replyto|contenttype|\bto\b", expr, re.I)


@pytest.mark.parametrize("topic,sub,expr", SUBS)
def test_filter_is_valid_sql_and_uses_only_properties_our_messages_carry(topic, sub, expr):
    tree = sqlglot.parse_one(f"SELECT 1 WHERE {expr}")
    used = {c.name for c in tree.find_all(exp.Column)}
    carried = set(M.alert_message(dict(alert_id="x", vehicle_id="v", rule="r", severity="s")).application_properties) if topic == "fleet-alerts" \
        else set(M.result_message(dict(message_id="x", route="r", severity="s")).application_properties)
    assert used and used <= carried, (used, carried)


def route(topic, props):
    """Evaluate every subscription filter of a topic against message properties (SQLite as the SQL92 evaluator)."""
    hits = set()
    cols = ", ".join(f"'{v}' AS {k}" for k, v in props.items())
    for t, sub, expr in SUBS:
        if t == topic and sqlite3.connect(":memory:").execute(f"SELECT 1 FROM (SELECT {cols}) WHERE {expr}").fetchone():
            hits.add(sub)
    return hits


def props_of(msg):
    return dict(msg.application_properties)


def test_alert_routing():
    mk = lambda sev: props_of(M.alert_message(dict(alert_id="x", vehicle_id="v", rule="r", severity=sev)))
    assert route("fleet-alerts", mk("CRITICAL")) == {"pager"}
    assert route("fleet-alerts", mk("WARNING")) == {"ops"}
    assert route("fleet-alerts", mk("INFO")) == set()


@pytest.mark.parametrize("r,s,want", [("QUARANTINED", "ERROR", {"ops"}), ("DUPLICATE", "WARNING", {"ops"}), ("ACCEPTED", "INFO", {"downstream"}),
                                      ("PARTIAL", "WARNING", {"downstream"}), ("ACCEPTED", "CRITICAL", {"downstream", "compliance"}),   # accepted AND critical
                                      ("QUARANTINED", "CRITICAL", {"ops", "compliance"})])
def test_dq_routing_fans_out_one_result_to_three_audiences(r, s, want):
    assert route("dq-results", props_of(M.result_message(dict(message_id="x", route=r, severity=s)))) == want


def test_message_id_is_deterministic_so_replays_can_be_absorbed():
    a = dict(alert_id="abc123", vehicle_id="V-1", rule="TEMP_CRITICAL", severity="CRITICAL")
    assert M.alert_message(a).message_id == M.alert_message(dict(a)).message_id == "abc123"
    assert M.alert_message(a).subject == "TEMP_CRITICAL" and json.loads(str(M.alert_message(a)))["vehicle_id"] == "V-1"
    assert M.result_message(dict(message_id="f:9", route="ACCEPTED", severity="INFO")).message_id == "f:9"
