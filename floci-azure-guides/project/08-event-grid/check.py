"""python check.py   (Floci AZ + hook_server.py + provision.py done) -> publish 3 events, only the 2 inbox ones may arrive."""
import json
import sys
import time
import urllib.request

import grid

endpoint, key = grid.create_topic(), grid.list_key()          # idempotent: PUT returns the existing topic
grid.publish(endpoint, key, [grid.blob_created("inbox", "BAN_20261002_001.csv"), grid.blob_created("inbox", "BAN_20261002_002.csv"),
                             grid.blob_created("accepted", "BAN/dt=2026-10-02/BAN_20261002_001.csv")])      # an OUTPUT of the gate: must not come back
for _ in range(20):
    got = json.loads(urllib.request.urlopen("http://localhost:8099/received", timeout=5).read())
    if len(got["processed"]) >= 2:
        break
    time.sleep(1)
results = {"both inbox events delivered": sorted(got["processed"]) == ["BAN_20261002_001.csv", "BAN_20261002_002.csv"],
           "output-zone event filtered out before delivery": got["ignored"] == 0}
for k, v in results.items():
    print("PASS" if v else "FAIL", k)
sys.exit(0 if all(results.values()) else 1)
