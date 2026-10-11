from decimal import Decimal

import pytest
import sqlglot

import connect
import load as ld
import schema


class Rec:                                                          # records statements instead of running them
    def __init__(self):
        self.sql, self.params = [], []

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self.params.append(params or [])

    def commit(self):
        pass

    def rollback(self):
        pass


def _dims_facts(n=2, batch="b-1"):
    dims = {"dim_plan": (ld.columns("dim_plan"), [[1, "PL01", "HMO", Decimal("0.30")], [2, "PL02", "PPO", Decimal("0.20")]])}
    rows = [[f"C-{i}", 1, Decimal("100.00"), Decimal("70.00"), "f.csv.gz", batch] for i in range(n)]
    return dims, {"fact_claim": (ld.columns("fact_claim"), rows)}


# ---- T-SQL validity (Azure SQL speaks T-SQL; SQLite does not) -------------------------------------------------
def test_every_loader_statement_parses_as_tsql():
    rec = Rec()
    dims, facts = _dims_facts()
    ld.load_batch(rec, dims, facts)
    stmts = [s.replace("?", "@p") for s in rec.sql] + [c for _, _, c in schema.create_statements()]
    assert len(stmts) >= 8
    for s in stmts:
        sqlglot.parse_one(s, read="tsql")                           # raises on a syntax error


def test_idempotent_ddl_wraps_a_valid_create_and_guards_every_object():
    ddl = schema.tsql_ddl()
    assert [s for s in ddl if s.startswith("IF SCHEMA_ID")] and all(s.startswith("IF ") for s in ddl)
    assert len(ddl) == 2 + len(schema.create_statements())
    for s in ddl:
        if "CREATE TABLE" in s:
            sqlglot.parse_one(s[s.index("CREATE TABLE"):], read="tsql")


def test_insert_respects_sql_server_parameter_limit():
    rec = Rec()
    ld.insert_rows(rec, "dw.t", ["a", "b", "c", "d"], [["1"] * 4] * 1500)
    assert max(len(p) for p in rec.params) <= 2100 and sum(len(p) for p in rec.params) == 6000


def test_odbc_string_quotes_awkward_passwords():
    s = connect.odbc_string("localhost", 1433, "demo", "sa", "p;w}d=!")
    assert "PWD={p;w}}d=!};" in s and "SERVER=localhost,1433" in s and "TrustServerCertificate=yes" in s


# ---- behaviour (the same SQL, executed) -----------------------------------------------------------------------
def count(conn, sql):
    return conn.execute(sql).fetchone()[0]


def test_load_then_reconcile(conn):
    dims, facts = _dims_facts(3)
    ld.load_batch(conn, dims, facts)
    assert ld.reconcile(conn, "b-1", {"fact_claim": 3}, "210.00") == {"loaded": {"fact_claim": 3}, "plan_paid": "210.00"}


def test_reload_replaces_never_duplicates(conn):
    dims, facts = _dims_facts(3)
    ld.load_batch(conn, dims, facts)
    ld.load_batch(conn, dims, facts)
    assert count(conn, "SELECT COUNT(*) FROM dw.fact_claim") == 3 and count(conn, "SELECT COUNT(*) FROM dw.dim_plan") == 2


def test_redelivered_row_replaces_its_old_version(conn):
    paid = lambda: float(count(conn, "SELECT plan_paid_total FROM dw.fact_claim WHERE claim_id = 'C-0'"))
    dims, facts = _dims_facts(1)
    ld.load_batch(conn, dims, facts)
    assert paid() == 70.0
    facts["fact_claim"][1][0][3] = Decimal("999.00")                # same claim_id, new plan_paid_total (column index 3)
    ld.load_batch(conn, dims, facts)
    assert paid() == 999.0 and count(conn, "SELECT COUNT(*) FROM dw.fact_claim") == 1     # replaced, not appended


def test_failure_mid_load_rolls_back_dimensions_and_facts(conn, monkeypatch):
    real = ld.insert_rows

    def boom(cur, table, *a, **k):
        if table == "stg.fact_claim":
            raise RuntimeError("simulated SQL outage mid-load")
        return real(cur, table, *a, **k)

    monkeypatch.setattr(ld, "insert_rows", boom)
    dims, facts = _dims_facts()
    with pytest.raises(RuntimeError):
        ld.load_batch(conn, dims, facts)
    assert count(conn, "SELECT COUNT(*) FROM dw.dim_plan") == 0 and count(conn, "SELECT COUNT(*) FROM dw.fact_claim") == 0   # dimensions rolled back too


def test_reconcile_mismatch_is_a_loud_failure(conn):
    dims, facts = _dims_facts(2)
    ld.load_batch(conn, dims, facts)
    with pytest.raises(ld.ReconciliationError):
        ld.reconcile(conn, "b-1", {"fact_claim": 3}, "140.00")      # transform claims 3 rows; warehouse has 2
    with pytest.raises(ld.ReconciliationError):
        ld.reconcile(conn, "b-1", {"fact_claim": 2}, "141.00")      # the right count but the wrong money
