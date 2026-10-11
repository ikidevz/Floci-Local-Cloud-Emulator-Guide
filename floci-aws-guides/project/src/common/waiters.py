import time
from typing import Callable, Iterable


def wait_until(check: Callable[[], str], *, done: Iterable[str], failed: Iterable[str] = (),
               timeout: int = 120, interval: float = 1.0) -> str:
    """Poll check() (returns a status string) until it reaches a done or failed state."""
    done, failed = set(done), set(failed)
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        status = check()
        if status in done:
            return status
        if status in failed:
            raise RuntimeError(f"Reached failure state: {status}")
        time.sleep(interval)
    raise TimeoutError(f"Timed out after {timeout}s (last status: {status})")
