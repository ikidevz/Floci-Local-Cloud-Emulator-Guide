"""python demo.py   (after `python provision.py` and exporting its output)
Creates the schema, loads a batch twice, proves the second load changed nothing, then reconciles."""
from decimal import Decimal

import connect
import load as ld
import schema

conn = connect.connect_from_env()
cur = conn.cursor()
for stmt in schema.tsql_ddl():                                      # idempotent
    cur.execute(stmt)
conn.commit()

dims = {"dim_plan": (ld.columns("dim_plan"), [[1, "PL01", "HMO", Decimal("0.30")], [2, "PL02", "PPO", Decimal("0.20")]])}
facts = {"fact_claim": (ld.columns("fact_claim"), [["C-1", 1, Decimal("100.00"), Decimal("70.00"), "claims_20260930.csv.gz", "b-1"],
                                                    ["C-2", 2, Decimal("50.00"), Decimal("40.00"), "claims_20260930.csv.gz", "b-1"]])}
for attempt in (1, 2):
    ld.load_batch(conn, dims, facts)
    print(f"load #{attempt}:", ld.reconcile(conn, "b-1", {"fact_claim": 2}, "110.00"))
cur.execute("SELECT COUNT(*) FROM dw.fact_claim")
assert cur.fetchone()[0] == 2, "re-load duplicated rows"
print("OK: re-load replaced, did not duplicate")
