"""The warehouse is defined ONCE, as data. CREATE TABLEs and load statements are generated from it, so they cannot drift apart.
Two tables are enough to carry the pattern: a dimension (full refresh) and a fact (stage -> delete by key -> insert)."""
SPEC = {
    "dim_plan": [("plan_key", "BIGINT NOT NULL"), ("plan_id", "NVARCHAR(10)"), ("plan_type", "NVARCHAR(12)"), ("coinsurance", "DECIMAL(4,2)")],
    "fact_claim": [("claim_id", "NVARCHAR(20) NOT NULL"), ("plan_key", "BIGINT"), ("billed_total", "DECIMAL(14,2)"),
                   ("plan_paid_total", "DECIMAL(14,2)"), ("source_file", "NVARCHAR(40)"), ("batch_id", "NVARCHAR(30)")],
}
DIMS = ["dim_plan"]
FACTS = {"fact_claim": "claim_id"}                                  # fact table -> delete key
PK = {"dim_plan": "plan_key", "fact_claim": "claim_id"}


def create_statements():
    """[(schema, table, CREATE TABLE ...)]. dw.* carry a primary key; stg.* (load staging) do not."""
    out = []
    for t, cols in SPEC.items():
        body = ", ".join(f"{c} {ty}" for c, ty in cols)
        out.append(("dw", t, f"CREATE TABLE dw.{t} ({body}, PRIMARY KEY ({PK[t]}))"))
        if t in FACTS:
            out.append(("stg", t, f"CREATE TABLE stg.{t} ({body.replace(' NOT NULL', '')})"))
    return out


def tsql_ddl():
    """Idempotent T-SQL: a schema/table is created only if it is missing, so `up` can run any number of times."""
    sql = [f"IF SCHEMA_ID('{s}') IS NULL EXEC('CREATE SCHEMA {s}')" for s in ("dw", "stg")]
    return sql + [f"IF OBJECT_ID('{s}.{t}', 'U') IS NULL {create}" for s, t, create in create_statements()]
