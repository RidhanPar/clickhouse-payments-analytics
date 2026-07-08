"""
Times the analytical queries behind the dashboard against ClickHouse.

Method: one warmup run per query (so all queries compete on the same page
cache footing), then 5 timed runs, reporting the median wall time measured
client side over HTTP. Rows and bytes read come from system.query_log, which
records what the server actually scanned, so index and partition pruning
effects are visible rather than guessed at.

    py -3.11 scripts/benchmark.py

Prints a markdown table ready to paste into the README.
"""

import statistics
import time
import uuid

from load_data import get_client

RUNS = 5

QUERIES = [
    ("Monthly volume and value, full 24 months",
     """SELECT toStartOfMonth(transaction_date) AS month,
               count() AS txns, sum(amount) AS amount
        FROM payments.transactions GROUP BY month ORDER BY month"""),

    ("One merchant's daily history (primary key hit)",
     """SELECT transaction_date, count() AS txns, sum(amount) AS amount
        FROM payments.transactions
        WHERE merchant_id = 'MCH102479'
        GROUP BY transaction_date ORDER BY transaction_date"""),

    ("Top 15 merchants by successful volume",
     """SELECT merchant_id, sumIf(amount, status = 'Success') AS volume
        FROM payments.transactions
        GROUP BY merchant_id ORDER BY volume DESC LIMIT 15"""),

    ("Failure reasons in Q1 2026 (partition pruning)",
     """SELECT failure_reason, count() AS failures
        FROM payments.transactions
        WHERE status = 'Failed'
          AND transaction_date BETWEEN '2026-01-01' AND '2026-03-31'
        GROUP BY failure_reason ORDER BY failures DESC"""),

    ("Daily failures from the base table",
     """SELECT transaction_date, countIf(status = 'Failed') AS failed
        FROM payments.transactions
        GROUP BY transaction_date ORDER BY transaction_date"""),

    ("Daily failures from the materialized view",
     """SELECT day, sum(failed_count) AS failed
        FROM payments.daily_merchant_stats
        GROUP BY day ORDER BY day"""),

    ("Full RFM segmentation, computed live",
     """SELECT segment, count() AS merchants
        FROM payments.merchant_segments GROUP BY segment"""),
]


def bench(client, label, sql):
    tag = uuid.uuid4().hex
    tagged = f"{sql} -- bench:{tag}"

    client.query(tagged)  # warmup
    times = []
    for _ in range(RUNS):
        start = time.perf_counter()
        client.query(tagged)
        times.append((time.perf_counter() - start) * 1000)

    client.command("SYSTEM FLUSH LOGS")
    stats = client.query(f"""
        SELECT median(query_duration_ms), max(read_rows),
               max(formatReadableSize(read_bytes))
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query LIKE '%bench:{tag}%'
          AND query NOT LIKE '%query_log%'
    """).result_rows[0]

    return statistics.median(times), stats[0], stats[1], stats[2]


def main():
    client = get_client()
    total_rows = client.command("SELECT count() FROM payments.transactions")
    print(f"Benchmarking against {total_rows:,} transactions, "
          f"median of {RUNS} runs after warmup, wall time over HTTP.\n")

    print("| Query | Server execution | End to end (HTTP) | Rows read | Data read |")
    print("|---|---|---|---|---|")
    for label, sql in QUERIES:
        wall_ms, server_ms, read_rows, read_bytes = bench(client, label, sql)
        print(f"| {label} | {server_ms:.0f} ms | {wall_ms:.0f} ms "
              f"| {read_rows:,} | {read_bytes} |")


if __name__ == "__main__":
    main()
