# Stack Setup and Configuration

This document explains every configuration choice in [docker-compose.yml](../docker-compose.yml). The goal is that someone maintaining this stack can answer "why is it set up this way" without reverse engineering the files.

## Overview

Four services on one Compose network:

| Service | Image | Purpose | Host ports |
|---|---|---|---|
| `clickhouse` | `clickhouse/clickhouse-server:24.8` | Analytical database holding the transactions | 8123, 9000 |
| `superset` | built from `superset/Dockerfile` | BI web server (UI and REST API) | 8088 |
| `superset-init` | same custom image | One-shot bootstrap job, exits after running | none |
| `superset-metadata-db` | `postgres:15-alpine` | Superset's own metadata store | none |

Compose puts all services on a default network where each service is reachable by its service name. That is why Superset's connection string points at the host `clickhouse`, not `localhost`: from inside the Superset container, `localhost` is the Superset container itself.

## First run

```bash
cp .env.example .env        # optional for local use, defaults work
docker compose up -d --build
```

The first start takes a few minutes: Docker builds the custom Superset image, `superset-init` migrates the metadata schema and creates the admin user, and only then does the `superset` service start. Once `docker compose ps` shows `payments-superset` as healthy:

- Superset UI: http://localhost:8088 (login `admin` / `admin_local` unless changed in `.env`)
- ClickHouse HTTP: http://localhost:8123/ping should return `Ok.`

## Image choices and pinning

- **ClickHouse 24.8** is an LTS release. Pinning a minor version instead of `latest` means the schema and benchmarks in this repo were tested against a known server version and a rebuild months from now behaves the same.
- **Superset 5.0.0** is pinned for the same reason, and because Superset dashboard export files are version sensitive. The export committed in this repo imports cleanly into 5.0.0.
- **Postgres 15 alpine** only stores Superset metadata, so the smallest maintained image is fine.

## Why a custom Superset image

The official Superset image ships with no database drivers at all, not even one for its own metadata store. The [superset/Dockerfile](../superset/Dockerfile) installs two packages and copies in `superset_config.py`:

- `clickhouse-connect`: the low level ClickHouse client plus the `clickhousedb://` SQLAlchemy dialect Superset uses to query the analytics database.
- `psycopg2-binary`: the Postgres driver Superset needs to reach its metadata database. Without it the server fails on startup with `ModuleNotFoundError: No module named 'psycopg2'`, which is worth knowing because it is the first thing that breaks when someone swaps the metadata store from SQLite to Postgres.

One trap worth documenting: the Superset 5 image runs the application from a virtualenv at `/app/.venv`, but the image's default `pip` belongs to the system Python. A plain `RUN pip install` succeeds and still leaves Superset unable to import the packages. The Dockerfile therefore installs with `uv pip install --python /app/.venv/bin/python`, which targets the interpreter Superset actually runs on.

Installing drivers at image build time, rather than exec'ing pip into a running container, means `docker compose up --build` on a fresh machine produces a working stack with no manual steps and no state that lives outside version control.

## Ports

- **8123 (ClickHouse HTTP)**: the interface `clickhouse-connect` speaks. Both Superset and the Python load script use it. It is also the simplest health probe (`/ping`).
- **9000 (ClickHouse native TCP)**: the binary protocol used by `clickhouse-client`. Exposed for ad hoc queries and the benchmark runs, since the native client reports server side execution time.
- **8088 (Superset)**: gunicorn serving the UI and the REST API.
- The metadata Postgres has **no host port** on purpose. Only Superset talks to it, over the internal network. Not publishing it avoids a collision with the two Postgres instances I already run locally on 5432 and 5433, and keeps the metadata store unreachable from outside Docker.

## Volumes (what survives a restart)

| Volume | Mounted at | Holds |
|---|---|---|
| `clickhouse_data` | `/var/lib/clickhouse` | All ClickHouse table data and metadata |
| `superset_metadata` | `/var/lib/postgresql/data` | Dashboards, charts, users, connection configs |
| `superset_home` | `/app/superset_home` | Superset logs and local cache |

`docker compose down` keeps all three, so dashboards and loaded data survive. `docker compose down -v` deletes them, which is the clean slate reset: after that, `superset-init` recreates the admin user on the next start and the data load script must be re-run.

## Superset admin bootstrap

Superset does not create an admin account on its own. The `superset-init` service runs three commands in order and then exits:

1. `superset db upgrade` migrates the metadata database schema (Alembic migrations).
2. `superset fab create-admin` creates the admin user from the `SUPERSET_ADMIN_*` environment variables. On a second run this command fails because the user exists; the `|| true` makes that a no-op instead of a crash, which keeps the bootstrap idempotent.
3. `superset init` seeds the default roles (Admin, Alpha, Gamma) and permissions.

The `superset` web service declares `depends_on: condition: service_completed_successfully` on the init job, so gunicorn never starts against an unmigrated metadata schema.

## Secrets and environment variables

All secrets flow through environment variables with dev defaults in `docker-compose.yml` (`${VAR:-default}` syntax). The defaults exist so `docker compose up` works on a fresh clone; they are acceptable only because every port binds to localhost. For anything shared, copy `.env.example` to `.env` and set real values.

`SUPERSET_SECRET_KEY` deserves a note: it signs session cookies and encrypts database credentials stored in the metadata DB. Superset 5 refuses to start with its known default key, so [superset_config.py](../superset/superset_config.py) always reads it from the environment. Rotating it invalidates stored encrypted credentials, so rotate it before creating connections, not after.

## How Superset connects to ClickHouse

Registered in Superset (Settings, Database Connections) with this SQLAlchemy URI:

```
clickhousedb://analytics:<CLICKHOUSE_PASSWORD>@clickhouse:8123/payments
```

Piece by piece:

- `clickhousedb://` selects the clickhouse-connect dialect installed in the custom image.
- `analytics` is the ClickHouse user created by the server container on first start from `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` environment variables.
- `clickhouse:8123` is the Compose service name plus the HTTP port. The driver is HTTP based, so 8123, not the native 9000.
- `payments` is the database created on first start from `CLICKHOUSE_DB`.

## Day 2 operations

- Logs: `docker compose logs -f superset` (or `clickhouse`).
- Restart one service: `docker compose restart superset`.
- Upgrade Superset: bump the tag in `superset/Dockerfile`, then `docker compose up -d --build`. The init job re-runs `superset db upgrade`, which applies any new metadata migrations. Take a copy of the `superset_metadata` volume first.
- Re-run bootstrap manually: `docker compose run --rm superset-init`.
- Health: ClickHouse answers on `http://localhost:8123/ping`, Superset on `http://localhost:8088/health`.
