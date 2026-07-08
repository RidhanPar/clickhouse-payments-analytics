"""
End to end verification of the running system. Run with the stack up and the
producer streaming:

    py -3.11 scripts/verify_live.py [--wait-anomaly MINUTES]

Checks, in order:

1. Retention is configured: the events and minute stats tables carry the 90
   day TTL in their definitions.
2. Events are flowing: the raw count grows over a 45 second observation
   window, and ingestion lag stays under 30 seconds.
3. Materialized views are exactly consistent with the raw stream: total
   events equals the minute rollup sum equals the daily rollup sum over the
   stream's date range. Insert-trigger MVs cannot drift; any mismatch means
   someone bypassed them.
4. Storage is healthy: the events table holds its rows in a small number of
   active parts (insert batching working).
5. Anomaly detection catches ground truth: the injected burst merchant is
   identified from the raw data itself (failure rate above 40% on 100+
   events in 10 minutes, which no natural merchant produces) and must appear
   on the merchant_anomalies watchlist. With --wait-anomaly the script polls
   until a burst happens (the producer injects one roughly every 15
   minutes), because verifying detection requires something to detect.

Exits non-zero if any check fails, so it can gate automation.
"""

import argparse
import sys
import time

from ch import get_client

FLOW_WINDOW_SECONDS = 45

results = []


def check(name, ok, detail):
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")


def check_ttl(client):
    for table in ("events", "platform_minute_stats"):
        ddl = client.command(f"SHOW CREATE TABLE payments.{table}")
        ok = "toIntervalDay(90)" in ddl and "ttl_only_drop_parts" in ddl
        check(f"TTL on {table}", ok,
              "90 day TTL with ttl_only_drop_parts" if ok else "TTL missing from DDL")


def check_flow(client):
    c0 = client.command("SELECT count() FROM payments.events")
    time.sleep(FLOW_WINDOW_SECONDS)
    c1 = client.command("SELECT count() FROM payments.events")
    delta = c1 - c0
    check("events flowing", delta > 0,
          f"{delta} events in {FLOW_WINDOW_SECONDS}s ({delta / FLOW_WINDOW_SECONDS:.1f}/s)")

    lag = client.command(
        "SELECT dateDiff('second', max(event_time), now()) FROM payments.events")
    check("ingestion lag", lag < 30, f"{lag}s behind wall clock (threshold 30s)")


def check_mv_consistency(client):
    raw, minute_sum, daily_sum = client.query("""
        SELECT
            (SELECT count() FROM payments.events),
            (SELECT sum(txn_count) FROM payments.platform_minute_stats),
            (SELECT sum(txn_count) FROM payments.daily_merchant_stats
             WHERE day >= (SELECT min(toDate(event_time)) FROM payments.events))
    """).result_rows[0]
    ok = raw == minute_sum == daily_sum
    check("materialized view consistency", ok,
          f"events={raw:,} minute_rollup={minute_sum:,} daily_rollup={daily_sum:,}")


def check_parts(client):
    parts, rows = client.query("""
        SELECT count(), sum(rows) FROM system.parts
        WHERE database = 'payments' AND table = 'events' AND active
    """).result_rows[0]
    check("insert batching (active parts)", parts < 50,
          f"{rows:,} rows in {parts} active parts")


def ground_truth_burst(client):
    """Identify a burst merchant from the data alone: no natural merchant
    exceeds a 40% failure rate on 100+ events in 10 minutes (the generator
    clips natural decline rates at 45% and no natural merchant gets that
    much traffic)."""
    rows = client.query("""
        SELECT merchant_id, count() AS n, countIf(status = 'Failed') / n AS rate
        FROM payments.events
        WHERE event_time > now() - INTERVAL 10 MINUTE
        GROUP BY merchant_id
        HAVING n >= 100 AND rate > 0.4
    """).result_rows
    return rows[0] if rows else None


def check_anomaly(client, wait_minutes):
    deadline = time.time() + wait_minutes * 60
    burst = ground_truth_burst(client)
    while burst is None and time.time() < deadline:
        remaining = int(deadline - time.time())
        print(f"      no burst active, waiting for the producer to inject one "
              f"({remaining}s left)...")
        time.sleep(30)
        burst = ground_truth_burst(client)

    if burst is None:
        check("anomaly detection", False,
              f"no burst occurred within {wait_minutes} min (producer injects "
              "every ~15 min; is it running?)")
        return

    merchant, n, rate = burst
    watchlist = {r[0] for r in client.query(
        "SELECT merchant_id FROM payments.merchant_anomalies").result_rows}
    check("anomaly detection", merchant in watchlist,
          f"ground truth burst {merchant} ({rate:.0%} over {n} events) "
          f"{'is' if merchant in watchlist else 'is NOT'} on the watchlist "
          f"(watchlist size {len(watchlist)})")
    check("watchlist precision", len(watchlist) <= 3,
          f"{len(watchlist)} merchants listed during burst (expect the burst "
          "plus at most stray 3 sigma noise)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait-anomaly", type=int, default=20, metavar="MINUTES",
                        help="how long to wait for an injected burst (default 20)")
    args = parser.parse_args()

    client = get_client()
    print("Verifying the live system end to end...\n")
    check_ttl(client)
    check_flow(client)
    check_mv_consistency(client)
    check_parts(client)
    check_anomaly(client, args.wait_anomaly)

    print(f"\n{sum(results)}/{len(results)} checks passed.")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
