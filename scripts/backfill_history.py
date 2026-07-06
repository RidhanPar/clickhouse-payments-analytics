"""
Seeds 24 months of history: the merchant dimension table and the daily
per-merchant rollup. Run once before starting the producer.

    py -3.11 scripts/backfill_history.py

Why this writes rollups, not raw events: the events table has a 90 day TTL,
so bulk-loading two years of raw history would be deleted by the next merge
cycle. History belongs in daily_merchant_stats, which has no TTL. The
materialized view cannot do this for us either, because an MV only processes
new inserts; it never re-reads existing data. So the backfill aggregates the
generated transactions to daily grain in pandas and inserts straight into
the MV's target table, which is the standard ClickHouse backfill pattern
(INSERT INTO target SELECT ... shaped like the MV query).

Idempotency: the history window (2024-07 through 2026-06) aligns exactly
with the rollup's monthly partitions, so a re-run drops those partitions
and reloads them. Partitions written by the live producer (2026-07 onward)
are never touched. This is why partition keys should align with reload
units: DROP PARTITION plus reinsert is the safe, instant way to redo a
backfill, with no mutations and no double counting.
"""

import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import generate_data  # noqa: E402
from ch import get_client  # noqa: E402

BATCH_SIZE = 100_000
HISTORY_PARTITIONS = [f"{y}{m:02d}" for y in (2024, 2025, 2026)
                      for m in range(1, 13)
                      if "202407" <= f"{y}{m:02d}" <= "202606"]


def build_frames():
    print(f"Generating {generate_data.N_MERCHANTS} merchants and 24 months of "
          "transactions (seeded)...")
    merchants = generate_data.generate_merchants(generate_data.N_MERCHANTS)
    transactions = generate_data.generate_transactions(merchants)

    merchants = merchants.drop(columns=["will_churn", "churn_day_offset"])
    merchants["signup_date"] = pd.to_datetime(merchants["signup_date"]).dt.date

    txns = transactions.assign(
        day=pd.to_datetime(transactions["transaction_date"]).dt.date,
        failed=(transactions["status"] == "Failed"),
    )
    txns["success_amount"] = txns["amount"].where(~txns["failed"], 0.0)
    daily = (
        txns.groupby(["merchant_id", "day"], as_index=False)
        .agg(txn_count=("transaction_id", "count"),
             failed_count=("failed", "sum"),
             success_amount=("success_amount", "sum"))
    )
    daily = daily[["day", "merchant_id", "txn_count", "failed_count", "success_amount"]]
    daily["success_amount"] = daily["success_amount"].round(2)
    return merchants, daily, len(transactions)


def main():
    merchants, daily, n_txns = build_frames()
    client = get_client()

    client.command("TRUNCATE TABLE payments.merchants")
    client.insert_df("payments.merchants", merchants)

    print(f"Dropping {len(HISTORY_PARTITIONS)} history partitions for an idempotent reload...")
    for p in HISTORY_PARTITIONS:
        client.command(f"ALTER TABLE payments.daily_merchant_stats DROP PARTITION '{p}'")

    print(f"Loading {len(daily):,} daily rollup rows "
          f"(aggregated from {n_txns:,} generated transactions)...")
    start = time.perf_counter()
    for offset in range(0, len(daily), BATCH_SIZE):
        client.insert_df("payments.daily_merchant_stats", daily.iloc[offset:offset + BATCH_SIZE])
    elapsed = time.perf_counter() - start

    total = client.command(
        "SELECT sum(txn_count) FROM payments.daily_merchant_stats "
        f"WHERE day < '{generate_data.WINDOW_END:%Y-%m-%d}'"
    )
    assert int(total) == n_txns, f"rollup mismatch: {total} vs {n_txns} generated"
    print(f"Merchants: {len(merchants):,}. History rollup rows: {len(daily):,} "
          f"in {elapsed:.2f}s. Transaction counts verified against the generator.")


if __name__ == "__main__":
    main()
