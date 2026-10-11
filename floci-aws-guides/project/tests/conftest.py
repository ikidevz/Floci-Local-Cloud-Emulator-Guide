import os
import uuid

# Unique prefix per test session so reruns never collide. Must run BEFORE src.common.aws is imported.
os.environ["PROJECT_PREFIX"] = "ft" + uuid.uuid4().hex[:6]

import pytest  # noqa: E402
from botocore.exceptions import EndpointConnectionError  # noqa: E402

from src.common.aws import client  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def emulator_up():
    try:
        client("sts").get_caller_identity()
    except EndpointConnectionError:
        pytest.exit("No AWS endpoint reachable. Start Floci: docker compose up -d", returncode=2)
