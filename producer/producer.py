"""
Continuous payment event producer.

Generates a realistic ongoing payment stream against the merchant book that
the backfill script seeded, and inserts it into payments.events. Designed to
run forever as a Compose service.

What "realistic" means here:
- Event volume follows a daily cycle (quiet nights, busy afternoons) around a
  configurable average rate.
- Each merchant keeps its own personality from the merchants table: traffic
  share proportional to its weekly transaction rate, ticket sizes lognormal
  around its average, failures at its own baseline decline rate.
- Occasionally one merchant enters an anomaly burst: its failure rate jumps
  to 55 to 80 percent and its traffic share is boosted, the way a broken
  payment integration or a fraud attack looks in real acquiring data. Bursts
  are logged to stdout so detection can be verified against ground truth.

Insert discipline, and why it matters more in ClickHouse than anywhere else:
every insert becomes an immutable part on disk that background merges must
later compact. Row by row inserts at 25 events/s would create ~2M parts a
day and kill the server. So events buffer in memory and flush every
FLUSH_INTERVAL_SECONDS as one batch, and the insert also sets async_insert=1
so the server coalesces batches from this and any other client into even
fewer parts. wait_for_async_insert=1 keeps the write durable: the flush only
returns once the server has committed the part, so a crashed insert is a
loud log line instead of silent data loss.
"""

import logging
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect

EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "25"))
FLUSH_INTERVAL_SECONDS = float(os.environ.get("FLUSH_INTERVAL_SECONDS", "5"))
ANOMALY_EVERY_MINUTES = float(os.environ.get("ANOMALY_EVERY_MINUTES", "15"))
ANOMALY_DURATION_MINUTES = float(os.environ.get("ANOMALY_DURATION_MINUTES", "7"))
ANOMALY_TRAFFIC_SHARE = float(os.environ.get("ANOMALY_TRAFFIC_SHARE", "0.05"))

PAYMENT_METHODS = ["Card", "Digital Wallet", "Bank Transfer"]
PAYMENT_METHOD_WEIGHTS = [0.68, 0.22, 0.10]
FAILURE_REASONS = ["Insufficient Funds", "Card Expired", "Fraud Block",
                   "Processor Timeout", "Invalid Details"]
FAILURE_REASON_WEIGHTS = [0.40, 0.15, 0.20, 0.15, 0.10]

COLUMNS = ["transaction_id", "merchant_id", "event_time", "amount",
           "payment_method", "status", "failure_reason"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "analytics"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "analytics_local"),
        database="payments",
    )


def load_merchants(client):
    """The producer refuses to invent merchants: it streams against the book
    seeded by the backfill, so live data joins cleanly with history.

    Traffic weights come from each merchant's actual volume over the trailing
    90 days of recorded history, not from the static weekly_txn_rate column.
    The difference matters: about a fifth of the generated book churned
    partway through history, and weighting by the static rate would resurrect
    every dead merchant the moment the stream starts, which destroys recency
    based segmentation. Merchants with no recent activity get no live
    traffic; the stream continues the book as it last behaved."""
    while True:
        rows = client.query(
            """SELECT m.merchant_id, m.avg_ticket_size, w.recent_txns, m.decline_rate
               FROM payments.merchants AS m
               INNER JOIN
               (
                   SELECT merchant_id, sum(txn_count) AS recent_txns
                   FROM payments.daily_merchant_stats
                   WHERE day >= (SELECT max(day) - 90 FROM payments.daily_merchant_stats)
                   GROUP BY merchant_id
                   HAVING recent_txns > 0
               ) AS w USING (merchant_id)"""
        ).result_rows
        if rows:
            return rows
        log.warning("no merchant history found; run scripts/backfill_history.py. Retrying in 10s")
        time.sleep(10)


def poisson(lam):
    """Knuth's algorithm; fine for the small rates used here."""
    l = math.exp(-lam)
    k, p = 0, 1.0
    while True:
        p *= random.random()
        if p <= l:
            return k
        k += 1


def daily_cycle(now):
    """Multiplier averaging 1.0 over a day: peak ~1.6 at 14:00 UTC, trough
    ~0.4 at 02:00 UTC. Payments traffic is diurnal; flat rates look fake."""
    hour = now.hour + now.minute / 60
    return 1 + 0.6 * math.sin(2 * math.pi * (hour - 8) / 24)


class AnomalyState:
    """At a random interval, one merchant goes bad for a few minutes."""

    def __init__(self, merchant_ids):
        # Bias bursts toward the busier half of the book so the burst rides
        # on top of enough traffic to be statistically detectable.
        self.candidates = merchant_ids
        self.active_merchant = None
        self.failure_rate = 0.0
        self.ends_at = 0.0
        self._schedule(time.time())

    def _schedule(self, now):
        self.next_at = now + random.uniform(0.7, 1.3) * ANOMALY_EVERY_MINUTES * 60

    def tick(self, now):
        if self.active_merchant and now >= self.ends_at:
            log.info("ANOMALY END merchant=%s", self.active_merchant)
            self.active_merchant = None
            self._schedule(now)
        elif not self.active_merchant and now >= self.next_at:
            self.active_merchant = random.choice(self.candidates)
            self.failure_rate = random.uniform(0.55, 0.80)
            self.ends_at = now + ANOMALY_DURATION_MINUTES * 60
            log.info("ANOMALY START merchant=%s failure_rate=%.2f duration=%.0fmin",
                     self.active_merchant, self.failure_rate, ANOMALY_DURATION_MINUTES)


def make_event(merchants, cum_weights, anomaly):
    m = random.choices(merchants, cum_weights=cum_weights, k=1)[0]
    merchant_id, avg_ticket, _, decline_rate = m

    failure_rate = decline_rate
    if anomaly.active_merchant:
        # During a burst, a slice of all traffic is redirected to the burst
        # merchant (failure storms usually come with retry storms).
        if random.random() < ANOMALY_TRAFFIC_SHARE:
            merchant_id = anomaly.active_merchant
            failure_rate = anomaly.failure_rate
        elif merchant_id == anomaly.active_merchant:
            failure_rate = anomaly.failure_rate

    amount = random.lognormvariate(math.log(max(float(avg_ticket), 1.0)), 0.35)
    amount = round(min(max(amount, 1.0), float(avg_ticket) * 8), 2)
    failed = random.random() < float(failure_rate)

    return (
        f"TXN{uuid.uuid4().hex[:20]}",
        merchant_id,
        datetime.now(timezone.utc).replace(tzinfo=None),
        amount,
        random.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS, k=1)[0],
        "Failed" if failed else "Success",
        random.choices(FAILURE_REASONS, weights=FAILURE_REASON_WEIGHTS, k=1)[0] if failed else "",
    )


def main():
    client = get_client()
    merchants = load_merchants(client)
    weights = [float(m[2]) for m in merchants]  # trailing 90 day volume as traffic share
    cum_weights = []
    total = 0.0
    for w in weights:
        total += w
        cum_weights.append(total)

    anomaly = AnomalyState([m[0] for m in merchants])
    heartbeat = Path("/tmp/heartbeat")
    heartbeat.touch()

    log.info("Streaming ~%.0f events/s (daily cycle applied) against %d merchants, "
             "flush every %.0fs", EVENTS_PER_SECOND, len(merchants), FLUSH_INTERVAL_SECONDS)

    buffer = []
    total_rows = 0
    last_flush = time.time()

    while True:
        tick_start = time.time()
        now_utc = datetime.now(timezone.utc)
        anomaly.tick(tick_start)

        rate = EVENTS_PER_SECOND * daily_cycle(now_utc)
        for _ in range(poisson(rate)):
            buffer.append(make_event(merchants, cum_weights, anomaly))

        if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS and buffer:
            try:
                client.insert(
                    "payments.events", buffer, column_names=COLUMNS,
                    settings={"async_insert": 1, "wait_for_async_insert": 1},
                )
                total_rows += len(buffer)
                log.info("flushed %d events (total %d)%s", len(buffer), total_rows,
                         f" [anomaly: {anomaly.active_merchant}]" if anomaly.active_merchant else "")
                buffer.clear()
                heartbeat.touch()
                last_flush = time.time()
            except Exception as exc:
                # Keep buffering and retry next interval; the heartbeat goes
                # stale if this persists, which flips the container unhealthy.
                log.error("flush failed, will retry: %s", exc)

        time.sleep(max(0.0, 1.0 - (time.time() - tick_start)))


if __name__ == "__main__":
    main()
