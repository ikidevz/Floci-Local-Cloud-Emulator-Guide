"""Warehouse connection from environment. pyodbc + 'ODBC Driver 18 for SQL Server' is the client Floci AZ documents.
The emulated server uses a self-signed certificate, hence TrustServerCertificate=yes (real Azure SQL: use a valid cert)."""
import os


def _esc(v):
    return "{" + str(v).replace("}", "}}") + "}"                    # ODBC brace-quoting: safe for ; = ! etc. in passwords


def odbc_string(host, port, db, user, password):
    return (f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host},{port};DATABASE={db};UID={_esc(user)};PWD={_esc(password)};"
            "Encrypt=yes;TrustServerCertificate=yes;")


def connect_from_env():
    import pyodbc                                                   # lazy: tests and DDL generation need no driver
    e = os.environ
    return pyodbc.connect(odbc_string(e.get("SQL_HOST", "localhost"), e["SQL_PORT"], e.get("SQL_DB", "demo"), e.get("SQL_USER", "sa"), e["SQL_PASSWORD"]),
                          autocommit=False, timeout=15)
