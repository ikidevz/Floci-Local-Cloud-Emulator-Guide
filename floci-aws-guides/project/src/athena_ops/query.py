from src.common.aws import client
from src.common.waiters import wait_until

athena = client("athena")


def run(sql: str, workgroup: str = "primary", database: str | None = None, timeout: int = 300) -> str:
    """Start a query, wait for it, return the QueryExecutionId. Raises with Athena's reason on failure."""
    kwargs = dict(QueryString=sql, WorkGroup=workgroup)
    if database:
        kwargs["QueryExecutionContext"] = {"Database": database}
    qid = athena.start_query_execution(**kwargs)["QueryExecutionId"]
    try:
        wait_until(lambda: athena.get_query_execution(QueryExecutionId=qid)
                   ["QueryExecution"]["Status"]["State"],
                   done={"SUCCEEDED"}, failed={"FAILED", "CANCELLED"}, timeout=timeout, interval=1)
    except RuntimeError as exc:
        raise RuntimeError(f"{exc}: {explain_failure(qid)}") from exc
    return qid


def explain_failure(qid: str) -> str:
    return athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"].get(
        "StateChangeReason", "")


def fetch(qid: str) -> list[dict]:
    """All rows as dicts. Athena returns the header as the first row of the first page."""
    rows, header = [], None
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            vals = [c.get("VarCharValue") for c in r["Data"]]
            if header is None:
                header = vals
                continue
            rows.append(dict(zip(header, vals)))
    return rows


def run_and_fetch(sql: str, workgroup: str = "primary", database: str | None = None) -> list[dict]:
    return fetch(run(sql, workgroup, database))


def to_dataframe(sql: str, workgroup: str = "primary", database: str | None = None):
    import pandas as pd
    return pd.DataFrame(run_and_fetch(sql, workgroup, database))
