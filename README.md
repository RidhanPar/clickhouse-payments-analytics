# Payments Analytics on ClickHouse and Apache Superset

A local real-time analytics stack: ClickHouse as the analytical database, Apache Superset as the BI layer, wired together with Docker Compose. The dataset is a simulated payments portfolio of about 575K transactions across 3,000 merchants over 24 months, reused from my [payment-merchant-clv-segmentation](https://github.com/RidhanPar/payment-merchant-clv-segmentation) project.

Built in stages, one pull request per stage:

1. Docker Compose stack and setup documentation
2. ClickHouse schema (MergeTree table, materialized view)
3. Batched data load
4. Superset dashboard, exported and committed
5. Query benchmarks

See [docs/SETUP.md](docs/SETUP.md) for how the stack is configured and [docs/SCHEMA.md](docs/SCHEMA.md) for the schema reasoning.

## Dataset and load

The generator is copied unchanged from the segmentation project (seeded with `numpy` seed 42, so every run produces the identical dataset): 3,000 merchants with lognormal ticket sizes and activity rates, industry dependent decline rates, and a churned subset, producing 575,226 transactions across 24 months.

```bash
pip install -r requirements.txt
py -3.11 scripts/load_data.py
```

The script generates the data in memory and inserts it over the HTTP interface in 100K row batches. Batching matters in ClickHouse: every insert becomes an immutable part on disk, so small frequent inserts create part counts that stall background merges. The load also verifies afterwards that the materialized view row counts match the base table.

Measured on this machine (Docker Desktop on Windows, ClickHouse 24.8):

| Metric | Value |
|---|---|
| Transactions loaded | 575,226 |
| Merchants loaded | 3,000 |
| Insert wall time | 2.92 s |
| Insert throughput | ~197,000 rows/s |
| Overall failure rate | 6.74% |
| On-disk size (transactions) | 6.41 MiB compressed (15.14 MiB uncompressed) |

The materialized view target ends up with 372,370 rows against 575,226 in the base table, a modest 1.5x reduction at this data density (many merchants transact less than daily). The reduction grows with volume; the point of the pattern is that the aggregate table grows with merchants times days while the base table grows with transactions.
