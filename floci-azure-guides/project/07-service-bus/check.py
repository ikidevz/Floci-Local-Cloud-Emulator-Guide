"""python check.py   (Floci AZ up with Config.json mounted)  -> publish, drain every subscription, assert the routing."""
import sys

import bus
import messages as M

ALERTS = [dict(alert_id="a-crit-1", vehicle_id="V-0024", rule="TEMP_CRITICAL", severity="CRITICAL"),
          dict(alert_id="a-warn-1", vehicle_id="V-0011", rule="DOOR_OPEN_TOO_LONG", severity="WARNING"),
          dict(alert_id="a-warn-2", vehicle_id="V-0001", rule="SENSOR_GAP", severity="WARNING")]
RESULTS = [dict(message_id="f1:aaaa", route="QUARANTINED", severity="ERROR"), dict(message_id="f2:bbbb", route="ACCEPTED", severity="INFO"),
           dict(message_id="f3:cccc", route="PARTIAL", severity="CRITICAL"), dict(message_id="f4:dddd", route="DUPLICATE", severity="WARNING")]
EXPECT = {("fleet-alerts", "pager"): {"a-crit-1"}, ("fleet-alerts", "ops"): {"a-warn-1", "a-warn-2"},
          ("dq-results", "ops"): {"f1:aaaa", "f4:dddd"}, ("dq-results", "compliance"): {"f3:cccc"}, ("dq-results", "downstream"): {"f2:bbbb", "f3:cccc"}}


def main():
    bus.publish("fleet-alerts", [M.alert_message(a) for a in ALERTS] + [M.alert_message(ALERTS[0])])        # the CRITICAL alert is sent TWICE (replay)
    bus.publish("dq-results", [M.result_message(r) for r in RESULTS])
    ok = True
    for (topic, sub), want in EXPECT.items():
        ids = [m for m, _ in bus.drain(topic, sub)]
        good = set(ids) == want
        ok &= good
        print("PASS" if good else "FAIL", f"{topic}/{sub}: got {sorted(set(ids))} want {sorted(want)}")
        if ids.count("a-crit-1") > 1:
            print(f"  INFO: duplicate delivered {ids.count('a-crit-1')}x - the emulator does not enforce duplicate detection (real Azure would deliver once)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
