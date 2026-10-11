import pathlib
import sys
import uuid

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import tablekit as tk  # noqa: E402


@pytest.fixture(scope="session")
def svc():
    """Auto-skips when no table endpoint answers (Floci AZ, or Azurite + STORAGE_CONNECTION_STRING)."""
    s = tk.service(connection_timeout=2, retry_total=0)
    try:
        list(s.list_tables(results_per_page=1))
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"no table endpoint reachable ({type(e).__name__}); run `docker compose up -d` or start Azurite")
    return s


@pytest.fixture
def t(svc):
    name = f"t{uuid.uuid4().hex[:12]}"
    tbl = svc.create_table(name)
    yield tbl
    svc.delete_table(name)
