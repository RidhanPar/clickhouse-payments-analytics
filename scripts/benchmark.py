"""
Times the analytical queries behind the live dashboards against ClickHouse,
while the producer is streaming.

Method: one warmup run per query (so all queries compete on the same page
cache footing), then 5 timed runs, reporting the median wall time measured
client side over HTTP. Rows and bytes read come from system.query_log, which
records what the server actually scanned, so partition pruning, primary key
hits, and rollup savings are measured rather than asserted.

    py -3.11 scripts/benchmark.py

Prints a markdown table ready to paste into the README.
"""

import statistics
import time
import uuid

from ch import get_client

RUNS = 5

QUERIES = [
    ("Per-minute volume, last 3h (minute rollup)",
     """SELECT minute, sum(txn_count) AS txns, sum(failed_count) AS failed
        FROM payments.platform_minute_stats
        WHERE minute > now() - INTERVAL 3 HOUR
        GROUP BY minute ORDER BY minute"""),

    ("Per-minute volume, last 3h (raw events)",
     """SELECT toStartOfMinute(event_time) AS minute,
               count() AS txns, countIf(status = 'Failed') AS failed
        FROM payments.events
        WHERE event_time > now() - INTERVAL 3 HOUR
        GROUP BY minute ORDER BY minute"""),

    ("Active merchants per minute, last 3h (uniqMerge)",
     """SELECT minute, uniqMerge(active_merchants) AS merchants
        FROM payments.platform_minute_stats
        WHERE minute > now() - INTERVAL 3 HOUR
        GROUP BY minute ORDER BY minute"""),

    ("One merchant's raw event history (primary key hit)",
     """SELECT toStartOfHour(event_time) AS hour, count() AS txns, sum(amount) AS amount
        FROM payments.events
        WHERE merchant_id = 'MCH102479'
        GROUP BY hour ORDER BY hour"""),

    ("Anomaly watchlist, full view",
     "SELECT * FROM payments.merchant_anomalies"),

    ("Monthly volume, 25 months (daily rollup)",
     """SELECT toStartOfMonth(day) AS month,
               sum(txn_count) AS txns, sum(success_amount) AS amount
        FROM payments.daily_merchant_stats
        GROUP BY month ORDER BY month"""),

    ("Daily failures, full history (daily rollup)",
     """SELECT day, sum(failed_count) AS failed
        FROM payments.daily_merchant_stats
        GROUP BY day ORDER BY day"""),

    ("Full RFM segmentation, computed live",
     """SELECT segment, count() AS merchants
        FROM payments.merchant_segments GROUP BY segment"""),
]


def bench(client, sql):
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
    events, span_start, span_end = client.query(
        "SELECT count(), min(event_time), max(event_time) FROM payments.events"
    ).result_rows[0]
    rollup = client.command("SELECT count() FROM payments.daily_merchant_stats")
    print(f"Benchmarking while streaming: {events:,} raw events "
          f"({span_start} to {span_end}), {rollup:,} daily rollup rows "
          f"(24 months history + live). Median of {RUNS} runs after warmup.\n")

    print("| Query | Server execution | End to end (HTTP) | Rows read | Data read |")
    print("|---|---|---|---|---|")
    for label, sql in QUERIES:
        wall_ms, server_ms, read_rows, read_bytes = bench(client, sql)
        print(f"| {label} | {server_ms:.0f} ms | {wall_ms:.0f} ms "
              f"| {read_rows:,} | {read_bytes} |")


if __name__ == "__main__":
    main()
