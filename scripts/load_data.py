"""
Generates the payments dataset in memory and loads it into ClickHouse.

Usage (stack must be up):
    py -3.11 scripts/load_data.py

Loading approach, and why:
- Data goes straight from the generator's DataFrames into ClickHouse over the
  HTTP interface using clickhouse-connect. No CSV intermediate: one less
  artifact to keep in sync, and the column types are converted once, here.
- Inserts are batched (100K rows per insert). ClickHouse writes each insert
  as an immutable part on disk, so row by row inserts would create hundreds of
  thousands of tiny parts and grind background merges to a halt. Large batches
  are the single most important ClickHouse insert habit.
- Tables are truncated first so the load is idempotent. The materialized view
  target is truncated explicitly: an MV only reacts to new inserts, so
  truncating the base table alone would leave stale aggregates behind.
"""

import os
import sys
import time

import pandas as pd
import clickhouse_connect

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import generate_data  # noqa: E402

BATCH_SIZE = 100_000


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "analytics"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "analytics_local"),
        database="payments",
    )


def prepare_frames():
    print(f"Generating {generate_data.N_MERCHANTS} merchants and their transactions (seeded)...")
    merchants = generate_data.generate_merchants(generate_data.N_MERCHANTS)
    transactions = generate_data.generate_transactions(merchants)

    merchants = merchants.drop(columns=["will_churn", "churn_day_offset"])
    merchants["signup_date"] = pd.to_datetime(merchants["signup_date"]).dt.date

    transactions["transaction_date"] = pd.to_datetime(transactions["transaction_date"]).dt.date
    # The schema stores '' for successful transactions instead of NULL.
    transactions["failure_reason"] = transactions["failure_reason"].fillna("")

    return merchants, transactions


def load(client, merchants, transactions):
    for table in ("transactions", "merchants", "daily_merchant_stats"):
        client.command(f"TRUNCATE TABLE payments.{table}")

    start = time.perf_counter()

    client.insert_df("payments.merchants", merchants)

    for offset in range(0, len(transactions), BATCH_SIZE):
        batch = transactions.iloc[offset : offset + BATCH_SIZE]
        client.insert_df("payments.transactions", batch)
        print(f"  inserted rows {offset + 1:>9,} .. {offset + len(batch):,}")

    elapsed = time.perf_counter() - start
    return elapsed


def verify(client, expected_txns):
    loaded = client.command("SELECT count() FROM payments.transactions")
    mv_rows = client.command(
        "SELECT sum(txn_count) FROM payments.daily_merchant_stats"
    )
    failed = client.command(
        "SELECT countIf(status = 'Failed') FROM payments.transactions"
    )
    assert loaded == expected_txns, f"row count mismatch: {loaded} loaded vs {expected_txns} generated"
    assert int(mv_rows) == expected_txns, (
        f"materialized view drifted: {mv_rows} counted vs {expected_txns} in base table"
    )
    return loaded, failed


def main():
    merchants, transactions = prepare_frames()
    client = get_client()

    print(f"Loading {len(transactions):,} transactions in batches of {BATCH_SIZE:,}...")
    elapsed = load(client, merchants, transactions)

    loaded, failed = verify(client, len(transactions))
    print()
    print(f"Merchants loaded:     {len(merchants):,}")
    print(f"Transactions loaded:  {loaded:,}")
    print(f"Failed transactions:  {failed:,} ({failed / loaded:.2%})")
    print(f"Insert wall time:     {elapsed:.2f}s ({loaded / elapsed:,.0f} rows/s)")
    print("Materialized view row counts match the base table.")


if __name__ == "__main__":
    main()
