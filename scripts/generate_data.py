"""
Generates a simulated payments dataset: merchants and their transaction history.

Copied verbatim from my payment-merchant-clv-segmentation project
(https://github.com/RidhanPar/payment-merchant-clv-segmentation) so both
projects analyze the same seeded dataset. Only this docstring differs. In this
repo the module is imported by load_data.py, which streams the DataFrames
straight into ClickHouse; running it directly still writes the CSVs.

Design notes (why the distributions look the way they do):
- Merchant transaction size is drawn from a lognormal distribution per merchant,
  which produces the long tail seen in real acquiring portfolios: most merchants
  process small tickets, a minority process large ones, and revenue concentrates
  in that minority.
- Merchant activity rate (transactions per week) is also lognormal and assigned
  independently of ticket size, so "high value" and "high volume" are not the
  same axis, mirroring how a real merchant book looks.
- Each merchant has its own baseline decline rate drawn from a beta distribution,
  with a smaller tail of high-risk merchants running much hotter decline rates
  (subscription and card-not-present businesses tend to see more declines).
- A subset of merchants stop transacting partway through the window, which is
  what makes recency vary naturally instead of being hardcoded into segments.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os

np.random.seed(42)

N_MERCHANTS = 3000
WINDOW_START = datetime(2024, 7, 1)
WINDOW_END = datetime(2026, 7, 1)  # 24 months
TOTAL_DAYS = (WINDOW_END - WINDOW_START).days

INDUSTRIES = [
    "E-commerce Retail", "Subscription / SaaS", "Food & Beverage",
    "Travel & Hospitality", "Professional Services", "Digital Goods",
    "Marketplace", "Utilities & Billing",
]
INDUSTRY_WEIGHTS = [0.24, 0.14, 0.16, 0.08, 0.12, 0.10, 0.10, 0.06]

# Some industries run hotter on declines (card-not-present, subscriptions, digital goods)
INDUSTRY_RISK_LIFT = {
    "E-commerce Retail": 1.0, "Subscription / SaaS": 1.6, "Food & Beverage": 0.7,
    "Travel & Hospitality": 1.3, "Professional Services": 0.6, "Digital Goods": 1.8,
    "Marketplace": 1.1, "Utilities & Billing": 0.9,
}

PAYMENT_METHODS = ["Card", "Digital Wallet", "Bank Transfer"]
PAYMENT_METHOD_WEIGHTS = [0.68, 0.22, 0.10]

FAILURE_REASONS = ["Insufficient Funds", "Card Expired", "Fraud Block", "Processor Timeout", "Invalid Details"]
FAILURE_REASON_WEIGHTS = [0.40, 0.15, 0.20, 0.15, 0.10]


def generate_merchants(n):
    merchant_ids = [f"MCH{100000 + i}" for i in range(n)]
    industries = np.random.choice(INDUSTRIES, size=n, p=INDUSTRY_WEIGHTS)

    # Signup dates staggered across a wider window so tenure varies realistically.
    signup_offsets = np.random.randint(0, TOTAL_DAYS - 30, size=n)
    signup_dates = [WINDOW_START + timedelta(days=int(o)) for o in signup_offsets]

    # Long-tail ticket size: most merchants small, a minority large.
    avg_ticket = np.random.lognormal(mean=3.6, sigma=1.0, size=n).clip(5, 5000)

    # Independent long-tail activity rate: transactions per week.
    weekly_txn_rate = np.random.lognormal(mean=0.9, sigma=1.0, size=n).clip(0.1, 150)

    # Baseline decline rate per merchant, lifted by industry risk.
    base_decline = np.random.beta(a=2, b=30, size=n)  # clusters low, small tail higher
    industry_lift = np.array([INDUSTRY_RISK_LIFT[i] for i in industries])
    decline_rate = (base_decline * industry_lift).clip(0.005, 0.45)

    # A subset of merchants churn (stop transacting) partway through the window.
    churns = np.random.random(n) < 0.22
    churn_offsets = np.random.randint(30, TOTAL_DAYS, size=n)

    merchants = pd.DataFrame({
        "merchant_id": merchant_ids,
        "industry": industries,
        "signup_date": signup_dates,
        "avg_ticket_size": avg_ticket.round(2),
        "weekly_txn_rate": weekly_txn_rate.round(2),
        "decline_rate": decline_rate.round(4),
        "will_churn": churns,
        "churn_day_offset": churn_offsets,
    })
    return merchants


def generate_transactions(merchants):
    all_txns = []
    txn_counter = 1

    for row in merchants.itertuples():
        active_start = row.signup_date
        active_end = WINDOW_END
        if row.will_churn:
            churn_date = WINDOW_START + timedelta(days=int(row.churn_day_offset))
            if churn_date > active_start:
                active_end = min(active_end, churn_date)

        active_days = (active_end - active_start).days
        if active_days <= 0:
            continue

        expected_txns = max(1, int(row.weekly_txn_rate * active_days / 7))
        # cap so a handful of extreme outliers don't blow up generation time
        expected_txns = min(expected_txns, 1200)

        offsets = np.random.randint(0, active_days, size=expected_txns)
        txn_dates = active_start + pd.to_timedelta(offsets, unit="D")

        amounts = np.random.lognormal(
            mean=np.log(max(row.avg_ticket_size, 1)), sigma=0.35, size=expected_txns
        ).clip(1, row.avg_ticket_size * 8)

        is_failed = np.random.random(expected_txns) < row.decline_rate
        methods = np.random.choice(PAYMENT_METHODS, size=expected_txns, p=PAYMENT_METHOD_WEIGHTS)
        fail_reasons = np.where(
            is_failed,
            np.random.choice(FAILURE_REASONS, size=expected_txns, p=FAILURE_REASON_WEIGHTS),
            None,
        )

        for i in range(expected_txns):
            all_txns.append((
                f"TXN{txn_counter:08d}",
                row.merchant_id,
                txn_dates[i].strftime("%Y-%m-%d"),
                round(float(amounts[i]), 2),
                methods[i],
                "Failed" if is_failed[i] else "Success",
                fail_reasons[i],
            ))
            txn_counter += 1

    txns = pd.DataFrame(all_txns, columns=[
        "transaction_id", "merchant_id", "transaction_date", "amount",
        "payment_method", "status", "failure_reason",
    ])
    return txns


def main():
    print(f"Generating {N_MERCHANTS} merchants over {TOTAL_DAYS} days...")
    merchants = generate_merchants(N_MERCHANTS)

    print("Generating transaction history (this can take a moment)...")
    transactions = generate_transactions(merchants)

    merchants_out = merchants.drop(columns=["will_churn", "churn_day_offset"]).copy()
    merchants_out["signup_date"] = merchants_out["signup_date"].dt.strftime("%Y-%m-%d")

    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(data_dir, exist_ok=True)

    merchants_out.to_csv(os.path.join(data_dir, "merchants.csv"), index=False)
    transactions.to_csv(os.path.join(data_dir, "transactions.csv"), index=False)

    print(f"Merchants: {len(merchants_out):,} rows -> data/merchants.csv")
    print(f"Transactions: {len(transactions):,} rows -> data/transactions.csv")
    print(f"Failure rate overall: {(transactions['status'] == 'Failed').mean():.2%}")


if __name__ == "__main__":
    main()
