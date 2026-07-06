# Payments Analytics on ClickHouse and Apache Superset

A local real-time analytics stack: ClickHouse as the analytical database, Apache Superset as the BI layer, wired together with Docker Compose. The dataset is a simulated payments portfolio of about 575K transactions across 3,000 merchants over 24 months, reused from my [payment-merchant-clv-segmentation](https://github.com/RidhanPar/payment-merchant-clv-segmentation) project.

Built in stages, one pull request per stage:

1. Docker Compose stack and setup documentation
2. ClickHouse schema (MergeTree table, materialized view)
3. Batched data load
4. Superset dashboard, exported and committed
5. Query benchmarks

See [docs/SETUP.md](docs/SETUP.md) for how the stack is configured and [docs/SCHEMA.md](docs/SCHEMA.md) for the schema reasoning.
