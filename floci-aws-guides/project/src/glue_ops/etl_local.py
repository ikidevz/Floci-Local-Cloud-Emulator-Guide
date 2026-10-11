"""
Run the Glue job's logic locally against the emulator.

Floci's StartJobRun follows Glue's state machine but does not execute the script, so
use this to actually produce the curated data (it calls the same clean() the Glue job uses).
"""
from glue_scripts.clean_orders_shell import main

from src.common.aws import name


def run_clean_orders(source_key: str = "raw/orders/orders.csv",
                     target_key: str = "clean/orders/orders.csv") -> None:
    main(["--SOURCE_BUCKET", name("raw"), "--SOURCE_KEY", source_key,
          "--TARGET_BUCKET", name("curated"), "--TARGET_KEY", target_key])
