"""Generate a small orders.csv (includes a duplicate and a bad amount on purpose)."""
import csv
import random
from pathlib import Path


def make(path: str = "data/orders.csv", n: int = 50, seed: int = 7) -> str:
    random.seed(seed)
    rows = [{"order_id": f"O-{i:04d}", "customer_id": f"C-{random.randint(1, 8)}",
             "amount": round(random.uniform(5, 500), 2), "status": "NEW"} for i in range(1, n + 1)]
    rows.append(dict(rows[0]))                                           # duplicate order_id
    rows.append({"order_id": "O-9999", "customer_id": "C-1", "amount": -5.0, "status": "NEW"})  # invalid
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["order_id", "customer_id", "amount", "status"])
        w.writeheader()
        w.writerows(rows)
    return path


if __name__ == "__main__":
    print(make())
