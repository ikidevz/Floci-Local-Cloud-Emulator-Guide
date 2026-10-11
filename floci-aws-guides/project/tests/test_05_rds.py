import os

import pytest

from src.common.aws import name
from src.rds_ops import db, instances, load
from src.s3_ops import buckets, objects

MOTO = os.getenv("EMULATOR") == "moto"
LOCAL_PG = bool(os.getenv("PG_HOST"))


@pytest.mark.skipif(MOTO and not LOCAL_PG, reason="needs real Postgres (Floci RDS, or PG_HOST)")
def test_rds_schema_load_and_query():
    ident = name("pg")
    if not LOCAL_PG:
        info = instances.create_postgres(ident)
        assert info["Endpoint"]["Port"]
    b = buckets.create_pipeline_buckets()
    objects.put_text(b["curated"], "clean/orders/orders.csv",
                     "order_id,customer_id,amount,status\nO-1,C-1,10.00,NEW\nO-2,C-1,5.50,NEW\n")
    load.apply_schema(ident)
    assert load.load_csv_from_s3(ident, b["curated"], "clean/orders/orders.csv") == 2
    assert load.load_csv_from_s3(ident, b["curated"], "clean/orders/orders.csv") == 2  # upsert, no dupes
    with db.connect(ident) as conn:
        rows = db.query(conn, "SELECT customer_id, sum(amount) AS total FROM orders GROUP BY 1")
    assert rows[0]["customer_id"] == "C-1" and float(rows[0]["total"]) == 15.5
