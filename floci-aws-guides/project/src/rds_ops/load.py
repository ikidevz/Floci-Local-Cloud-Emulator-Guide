import csv
import io
from pathlib import Path

import psycopg2.extras

from src.common.aws import client
from src.rds_ops.db import connect


def apply_schema(identifier: str, path: str = "sql/schema.sql") -> None:
    with connect(identifier) as conn, conn.cursor() as cur:
        cur.execute(Path(path).read_text())


def load_csv_from_s3(identifier: str, bucket: str, key: str) -> int:
    """Upsert curated orders into Postgres. Returns rows written."""
    body = client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    rows = [(r["order_id"], r["customer_id"], r["amount"], r.get("status") or "NEW")
            for r in csv.DictReader(io.StringIO(body))]
    with connect(identifier) as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO orders (order_id, customer_id, amount, status) VALUES %s
               ON CONFLICT (order_id) DO UPDATE
               SET amount = EXCLUDED.amount, status = EXCLUDED.status""",
            rows)
    return len(rows)
