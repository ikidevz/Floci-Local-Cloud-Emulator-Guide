"""The three Table Storage patterns the pipelines depend on:
  1. LEDGER   - upsert a row per file / per run, read it back by key        (batch ETL: etlledger)
  2. HOT STATE- read row + ETag, write back ONLY if nobody changed it       (streaming: fleetstate)
  3. CLAIM    - atomic create-if-absent, first writer wins                  (DQ gate: dqaudit 'hash' rows)
"""
import json

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError, ResourceNotFoundError
from azure.data.tables import TableServiceClient, UpdateMode

from floci import storage_conn_str


def service(**kw):
    return TableServiceClient.from_connection_string(storage_conn_str(), **kw)


def table(name, svc=None):
    return (svc or service()).create_table_if_not_exists(name)


# 1. ledger ---------------------------------------------------------------------------------------------
def ledger_put(t, key, partition="file", **props):
    t.upsert_entity({"PartitionKey": partition, "RowKey": key, **props})        # upsert: re-running a step overwrites its own row


def ledger_get(t, key, partition="file"):
    try:
        return t.get_entity(partition, key)
    except ResourceNotFoundError:
        return None


def ledger_list(t, partition):
    return list(t.query_entities(f"PartitionKey eq '{partition}'"))


# 2. hot state with optimistic concurrency ----------------------------------------------------------------
def read(t, pk, rk):
    """-> (entity | None, etag | None)"""
    try:
        e = t.get_entity(pk, rk)
        return e, e.metadata["etag"]
    except ResourceNotFoundError:
        return None, None


def write_if_unchanged(t, entity, etag):
    """True if written. False if another writer got there first (HTTP 412 on update, 409 on insert):
    the caller must re-read and redo its work. etag=None means 'insert; the row must not exist yet'."""
    try:
        if etag is None:
            t.create_entity(entity)
        else:
            t.update_entity(entity, mode=UpdateMode.REPLACE, etag=etag, match_condition=MatchConditions.IfNotModified)
        return True
    except (ResourceModifiedError, ResourceExistsError):
        return False


def pack(state: dict) -> str:
    """Whole state in one JSON property; keep a few columns beside it for querying (the 'hot row is a projection' pattern)."""
    return json.dumps(state, separators=(",", ":"))


# 3. atomic claim -----------------------------------------------------------------------------------------
def claim(t, pk, rk, **props):
    """Create-if-absent. True = you own it. False = someone already did (the DQ gate routes the second file DUPLICATE)."""
    try:
        t.create_entity({"PartitionKey": pk, "RowKey": rk, **props})
        return True
    except ResourceExistsError:
        return False
