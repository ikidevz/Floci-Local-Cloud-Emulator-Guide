"""Connection helper shared by the storage projects.

Floci AZ routes every service by PATH on one port:
    Blob   http://localhost:4577/<account>
    Table  http://localhost:4577/<account>-table        (NOT the blob path)
Set STORAGE_CONNECTION_STRING to point at Azurite or real Azure instead.
"""
import os

ACCOUNT = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="   # public Azurite dev key, not a secret


def conn_str(host="localhost", port=4577):
    return (f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};"
            f"BlobEndpoint=http://{host}:{port}/{ACCOUNT};"
            f"TableEndpoint=http://{host}:{port}/{ACCOUNT}-table;")


def storage_conn_str():
    return os.environ.get("STORAGE_CONNECTION_STRING") or conn_str()
