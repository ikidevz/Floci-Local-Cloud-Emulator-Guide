"""
Glue *Python shell* job: de-duplicate orders and drop non-positive amounts.

Glue passes arguments as:  --SOURCE_BUCKET b --SOURCE_KEY k --TARGET_BUCKET b --TARGET_KEY k
Self-contained on purpose: Glue runs this single file. The same clean() is reused by the local
runner (src/glue_ops/etl_local.py), because Floci's StartJobRun does NOT execute scripts.
"""
import csv
import io
import sys


def clean(rows: list[dict]) -> list[dict]:
    seen, out = set(), []
    for r in rows:
        try:
            amount = float(r["amount"])
        except (KeyError, ValueError):
            continue
        if amount <= 0 or r["order_id"] in seen:
            continue
        seen.add(r["order_id"])
        out.append({**r, "amount": f"{amount:.2f}"})
    return out


def to_csv(rows: list[dict], fieldnames: list[str]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def parse_args(argv: list[str]) -> dict:
    return {k.lstrip("-"): v for k, v in zip(argv[0::2], argv[1::2]) if k.startswith("--")}


def main(argv: list[str]) -> None:
    import boto3  # imported here so unit tests of clean() need no AWS
    a = parse_args(argv)
    s3 = boto3.client("s3")
    body = s3.get_object(Bucket=a["SOURCE_BUCKET"], Key=a["SOURCE_KEY"])["Body"].read().decode()
    reader = csv.DictReader(io.StringIO(body))
    rows = clean(list(reader))
    s3.put_object(Bucket=a["TARGET_BUCKET"], Key=a["TARGET_KEY"],
                  Body=to_csv(rows, reader.fieldnames).encode())
    print(f"kept {len(rows)} rows")


if __name__ == "__main__":
    main(sys.argv[1:])
