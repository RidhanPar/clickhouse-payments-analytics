# The official Superset image ships without database drivers. This image adds
# the two we need: clickhouse-connect (Superset to ClickHouse over HTTP) and
# psycopg2 (Superset to its Postgres metadata store), plus our config file.
FROM apache/superset:5.0.0

USER root
# Superset runs from the virtualenv at /app/.venv, but the image's default
# `pip` targets the system Python. Installing with plain pip silently puts
# packages where Superset never looks. Point uv at the venv interpreter.
RUN uv pip install --python /app/.venv/bin/python --no-cache \
    "clickhouse-connect==0.8.*" "psycopg2-binary==2.9.*"

# Superset picks up superset_config.py from /app/pythonpath automatically.
COPY superset_config.py /app/pythonpath/superset_config.py

USER superset
