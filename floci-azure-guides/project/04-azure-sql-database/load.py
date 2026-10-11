"""One-transaction warehouse load. Deliberately PLAIN SQL (DELETE ... WHERE IN, INSERT ... SELECT): valid T-SQL, and it runs
unchanged on SQLite, so the same statements are executed in the tests. Placeholder is '?' for both pyodbc and sqlite3."""
from decimal import Decimal

from schema import DIMS, FACTS, SPEC


class ReconciliationError(Exception):
    pass


def _plain(v):
    return str(v) if isinstance(v, Decimal) else v                  # Decimal -> str: drivers disagree about Decimal binding


def insert_rows(cur, table, cols, rows, max_params=2000):
    """Multi-row parameterised INSERT. SQL Server allows 2100 parameters per statement -> rows per statement = 2000 // columns."""
    per = max(1, max_params // len(cols))
    one = "(" + ",".join(["?"] * len(cols)) + ")"
    for i in range(0, len(rows), per):
        chunk = rows[i:i + per]
        cur.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES " + ",".join([one] * len(chunk)), [_plain(v) for r in chunk for v in r])


def load_batch(conn, dims, facts):
    """dims/facts: {table: (columns, rows)}. ATOMIC: a failure anywhere rolls back dimensions AND facts together.
    Dimensions: full refresh (surrogate keys are deterministic). Facts: stage -> delete by key -> insert, so a
    re-delivered row REPLACES its old version and a re-run never duplicates."""
    cur = conn.cursor()
    try:
        for t, (cols, rows) in dims.items():
            cur.execute(f"DELETE FROM dw.{t}")
            insert_rows(cur, f"dw.{t}", cols, rows)
        for t, (cols, rows) in facts.items():
            key, names = FACTS[t], ", ".join(cols)
            cur.execute(f"DELETE FROM stg.{t}")
            insert_rows(cur, f"stg.{t}", cols, rows)
            cur.execute(f"DELETE FROM dw.{t} WHERE {key} IN (SELECT {key} FROM stg.{t})")
            cur.execute(f"INSERT INTO dw.{t} ({names}) SELECT {names} FROM stg.{t}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def reconcile(conn, batch_id, expected_rows: dict, expected_plan_paid: str):
    """Control totals: what the warehouse holds for this batch must equal what the transform produced. A mismatch is a DATA failure."""
    cur = conn.cursor()
    got = {}
    for t in FACTS:
        cur.execute(f"SELECT COUNT(*) FROM dw.{t} WHERE batch_id = ?", [batch_id])
        got[t] = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(plan_paid_total), 0) FROM dw.fact_claim WHERE batch_id = ?", [batch_id])
    paid = Decimal(str(cur.fetchone()[0])).quantize(Decimal("0.01"))
    if got != expected_rows or paid != Decimal(expected_plan_paid).quantize(Decimal("0.01")):
        raise ReconciliationError(f"warehouse {got} / plan paid {paid} != transform {expected_rows} / {expected_plan_paid}")
    return {"loaded": got, "plan_paid": str(paid)}


def columns(table):
    return [c for c, _ in SPEC[table]]
