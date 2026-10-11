import pathlib
import sqlite3
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import schema  # noqa: E402


@pytest.fixture
def conn(tmp_path):
    """SQLite standing in for SQL Server: two attached databases play the schemas `dw` and `stg`."""
    c = sqlite3.connect(":memory:")
    for s in ("dw", "stg"):
        c.execute(f"ATTACH DATABASE '{tmp_path / (s + '.db')}' AS {s}")
    for _, _, create in schema.create_statements():
        c.execute(create)
    c.commit()
    return c
