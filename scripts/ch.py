"""Shared ClickHouse client for the repo's scripts (backfill, benchmark,
dashboard build). Connection details come from the same environment
variables the Compose stack uses, with matching local defaults."""

import os

import clickhouse_connect


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "analytics"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "analytics_local"),
        database="payments",
    )
