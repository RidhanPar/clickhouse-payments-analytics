"""
Superset server configuration. Superset imports this module on startup
because the Dockerfile places it on /app/pythonpath.

Only two things are strictly required for a working server:
- SECRET_KEY: signs session cookies and encrypts stored database credentials.
  Superset 5 refuses to start with the known default, so we always set one.
- SQLALCHEMY_DATABASE_URI: where Superset keeps its own metadata (users,
  dashboards, chart definitions). This is NOT the analytics database. We point
  it at the Postgres container; SQLite would work for a demo but Superset
  logs warnings and it cannot handle concurrent writes from gunicorn workers.
"""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_METADATA_DB_URI"]

# Cap rows fetched into the chart builder. ClickHouse will happily return
# millions of rows; Superset rendering will not.
ROW_LIMIT = 50000

# Local single-node setup: no Redis, so cache query results in memory per
# worker. Good enough for one analyst; a shared Redis cache is the first
# thing to add if this served a team.
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
