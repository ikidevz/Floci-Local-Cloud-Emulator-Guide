import pathlib
import sys
import uuid

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import blobzones as bz  # noqa: E402


@pytest.fixture(scope="session")
def svc():
    """Auto-skips when no storage endpoint answers (start Floci AZ, or Azurite + STORAGE_CONNECTION_STRING)."""
    s = bz.service(connection_timeout=2, retry_total=0)
    try:
        list(s.list_containers(results_per_page=1))
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"no blob endpoint reachable ({type(e).__name__}); run `docker compose up -d` or start Azurite")
    return s


@pytest.fixture
def box(svc):
    name = f"t-{uuid.uuid4().hex[:10]}"
    svc.create_container(name)
    yield name
    svc.delete_container(name)
