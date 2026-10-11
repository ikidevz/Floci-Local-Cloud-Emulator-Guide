import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from src.rds_ops.instances import endpoint


def _target(identifier: str) -> tuple[str, int]:
    """PG_HOST/PG_PORT override the RDS endpoint (handy for a local Postgres or inside containers)."""
    if os.getenv("PG_HOST"):
        return os.environ["PG_HOST"], int(os.getenv("PG_PORT", "5432"))
    return endpoint(identifier)  # use what Floci returns; the port is a proxy port, not 5432


@contextmanager
def connect(identifier: str, db_name: str = "appdb", user: str = "app", password: str = "ChangeMe123!"):
    host, port = _target(identifier)
    conn = psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password,
                            connect_timeout=10)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query(conn, sql: str, params: tuple | None = None) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()] if cur.description else []
