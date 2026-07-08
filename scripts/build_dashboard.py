"""
Builds both Superset dashboards through the REST API, then exports them.

Why a script instead of clicking: the dashboard definitions become code that
can be reviewed in a pull request and rebuilt from scratch, and the exported
zip (superset/exports/) is the import artifact for anyone cloning the repo.
Run with the stack up, history backfilled, and the producer streaming:

    py -3.11 scripts/build_dashboard.py

Two dashboards, matching the two workloads in docs/SCHEMA.md:

- "Payments Operations (Live)" refreshes itself every 30 seconds: per-minute
  volume and failure rate (from the AggregatingMergeTree rollup), distinct
  active merchants, today's top merchants (raw events), and the anomaly
  watchlist.
- "Payments History" reads only the daily rollup and the segment views;
  nothing on it touches raw events, so it stays fast forever regardless of
  stream volume, and it needs no auto refresh.

Idempotent: reuses the database connection, replaces charts, datasets and
dashboards by name, and deletes assets left behind by earlier revisions.
"""

import json
import os
import uuid

import requests

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")
USERNAME = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin_local")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "analytics_local")

DB_NAME = "ClickHouse payments"
OPS_DASHBOARD = "Payments Operations (Live)"
HISTORY_DASHBOARD = "Payments History"
MONITORING_DASHBOARD = "System Monitoring"
EXPORT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "superset", "exports", "payments_dashboards.zip",
)

# Assets from earlier revisions of this script, removed on each run so the
# Superset instance converges to exactly what this file describes.
LEGACY_CHARTS = [
    "Transaction volume over time",
    "Daily failed transactions (from materialized view)",
    "Failure rate by merchant segment",
    "Top merchants by processed volume",
    "Anomaly watchlist (failure rate vs merchant baseline)",
]
LEGACY_DATASETS = ["transactions", "transactions_with_segments"]
LEGACY_DASHBOARDS = ["Payments Portfolio Overview"]

# Segment labels join the rollup so failure rates can be grouped by segment.
ROLLUP_SEGMENTS_SQL = """\
SELECT
    d.day,
    d.merchant_id,
    d.txn_count,
    d.failed_count,
    d.success_amount,
    s.segment
FROM payments.daily_merchant_stats AS d
INNER JOIN payments.merchant_segments AS s USING (merchant_id)
"""

# Storage health straight from ClickHouse's own bookkeeping. Active part
# counts are the first thing to look at when inserts misbehave: a table
# accumulating hundreds of parts means batching is broken somewhere.
SYSTEM_PARTS_SQL = """\
SELECT
    table,
    count()                                              AS active_parts,
    sum(rows)                                            AS rows,
    round(sum(data_compressed_bytes) / 1048576, 2)       AS compressed_mib,
    round(sum(data_uncompressed_bytes) / 1048576, 2)     AS uncompressed_mib
FROM system.parts
WHERE database = 'payments' AND active
GROUP BY table
ORDER BY rows DESC
"""


class Superset:
    def __init__(self):
        self.s = requests.Session()
        r = self.s.post(f"{BASE}/api/v1/security/login", json={
            "username": USERNAME, "password": PASSWORD,
            "provider": "db", "refresh": True,
        })
        r.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        csrf = self.s.get(f"{BASE}/api/v1/security/csrf_token/")
        csrf.raise_for_status()
        self.s.headers["X-CSRFToken"] = csrf.json()["result"]
        self.s.headers["Referer"] = BASE

    def get(self, path, **kw):
        r = self.s.get(f"{BASE}/api/v1{path}", **kw)
        r.raise_for_status()
        return r

    def post(self, path, payload):
        r = self.s.post(f"{BASE}/api/v1{path}", json=payload)
        if not r.ok:
            raise RuntimeError(f"POST {path} failed {r.status_code}: {r.text[:500]}")
        return r.json()

    def put(self, path, payload):
        r = self.s.put(f"{BASE}/api/v1{path}", json=payload)
        if not r.ok:
            raise RuntimeError(f"PUT {path} failed {r.status_code}: {r.text[:500]}")
        return r.json()

    def delete(self, path):
        r = self.s.delete(f"{BASE}/api/v1{path}")
        r.raise_for_status()

    def find_one(self, resource, name_col, name):
        q = json.dumps({"filters": [{"col": name_col, "opr": "eq", "value": name}]})
        r = self.get(f"/{resource}/", params={"q": q}).json()
        return r["result"][0]["id"] if r["count"] else None


def ensure_database(api):
    db_id = api.find_one("database", "database_name", DB_NAME)
    if db_id:
        return db_id
    return api.post("/database/", {
        "database_name": DB_NAME,
        "sqlalchemy_uri": f"clickhousedb://analytics:{CLICKHOUSE_PASSWORD}@clickhouse:8123/payments",
        "expose_in_sqllab": True,
    })["id"]


def ensure_dataset(api, db_id, table_name, sql=None):
    ds_id = api.find_one("dataset", "table_name", table_name)
    if ds_id:
        api.delete(f"/dataset/{ds_id}")
    payload = {"database": db_id, "schema": "payments", "table_name": table_name}
    if sql:
        payload["sql"] = sql
    return api.post("/dataset/", payload)["id"]


def metric_sql(expression, label):
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def sql_filter(expression):
    """A custom SQL WHERE filter. Chosen over Superset's TEMPORAL_RANGE
    filters ('Last 3 hours' style) because those go through Superset's
    natural language date parser, which rejected the range grammar here;
    a SQL predicate needs no parsing and pushes straight down to ClickHouse."""
    return {"expressionType": "SQL", "clause": "WHERE", "sqlExpression": expression}


def axis_column(col, grain=None):
    c = {"columnType": "BASE_AXIS", "expressionType": "SQL",
         "label": col, "sqlExpression": col}
    if grain:
        c["timeGrain"] = grain
    return c


def build_query_context(dataset_id, params):
    """Derive the saved query context from the chart params, the way the UI
    does on save. Without it the /chart/{id}/data endpoint refuses to run."""
    if "x_axis" in params:
        columns = [axis_column(params["x_axis"], params.get("time_grain_sqla"))]
    elif params.get("query_mode") == "raw":
        columns = list(params.get("all_columns", []))
    else:
        columns = list(params.get("groupby", []))

    where = " AND ".join(
        f"({f['sqlExpression']})"
        for f in params.get("adhoc_filters", [])
        if f.get("expressionType") == "SQL" and f.get("clause") == "WHERE"
    )

    query = {
        "columns": columns,
        "filters": [],
        "row_limit": params.get("row_limit", 10000),
        "extras": {"having": "", "where": where},
    }
    metrics = params.get("metrics") or ([params["metric"]] if params.get("metric") else None)
    if metrics:
        query["metrics"] = metrics
    if params.get("time_grain_sqla"):
        query["extras"]["time_grain_sqla"] = params["time_grain_sqla"]
    if params.get("series_limit_metric"):
        query["orderby"] = [[params["series_limit_metric"], not params.get("order_desc", True)]]
    if params.get("query_mode") == "raw" and params.get("order_by_cols"):
        query["orderby"] = [json.loads(o) for o in params["order_by_cols"]]

    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [query],
        "form_data": params,
        "result_format": "json",
        "result_type": "full",
    }


def ensure_chart(api, name, dataset_id, viz_type, params):
    chart_id = api.find_one("chart", "slice_name", name)
    if chart_id:
        api.delete(f"/chart/{chart_id}")
    params = {"datasource": f"{dataset_id}__table", "viz_type": viz_type, **params}
    return api.post("/chart/", {
        "slice_name": name,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(params),
        "query_context": json.dumps(build_query_context(dataset_id, params)),
        "query_context_generation": True,
    })["id"]


def chart_component(chart_id, name, width, height):
    return {
        "type": "CHART",
        "id": f"CHART-{uuid.uuid4().hex[:10]}",
        "children": [],
        "meta": {"chartId": chart_id, "width": width, "height": height, "sliceName": name},
    }


def build_position_json(title, rows):
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
    }
    for i, row in enumerate(rows, start=1):
        row_id = f"ROW-{i}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "type": "ROW", "id": row_id,
            "children": [c["id"] for c in row],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for c in row:
            c["parents"] = ["ROOT_ID", "GRID_ID", row_id]
            position[c["id"]] = c
    return position


def ensure_dashboard(api, title, rows, refresh_frequency=0):
    dash_id = api.find_one("dashboard", "dashboard_title", title)
    if dash_id:
        api.delete(f"/dashboard/{dash_id}")
    dash_id = api.post("/dashboard/", {
        "dashboard_title": title,
        "position_json": json.dumps(build_position_json(title, rows)),
        # refresh_frequency is Superset's dashboard auto refresh in seconds;
        # 0 disables it. 30s matches the operational cadence: the per-minute
        # rollup gains a new row roughly every refresh.
        "json_metadata": json.dumps({"refresh_frequency": refresh_frequency}),
        "published": True,
    })["id"]
    for c in (c for row in rows for c in row):
        api.put(f"/chart/{c['meta']['chartId']}", {"dashboards": [dash_id]})
    return dash_id


def cleanup_legacy(api):
    for title in LEGACY_DASHBOARDS:
        did = api.find_one("dashboard", "dashboard_title", title)
        if did:
            api.delete(f"/dashboard/{did}")
    for name in LEGACY_CHARTS:
        cid = api.find_one("chart", "slice_name", name)
        if cid:
            api.delete(f"/chart/{cid}")
    for name in LEGACY_DATASETS:
        dsid = api.find_one("dataset", "table_name", name)
        if dsid:
            api.delete(f"/dataset/{dsid}")


def main():
    api = Superset()
    cleanup_legacy(api)

    db_id = ensure_database(api)
    print(f"Database connection ready (id {db_id})")

    ds_minute = ensure_dataset(api, db_id, "platform_minute_stats")
    ds_events = ensure_dataset(api, db_id, "events")
    ds_daily = ensure_dataset(api, db_id, "daily_merchant_stats")
    ds_anom = ensure_dataset(api, db_id, "merchant_anomalies")
    ds_segments = ensure_dataset(api, db_id, "merchant_segments")
    ds_rollup_seg = ensure_dataset(api, db_id, "rollup_with_segments", sql=ROLLUP_SEGMENTS_SQL)
    print("Datasets ready")

    # ---- Operations dashboard (live tables, 30s auto refresh) ----
    # Every chart on the minute rollup re-aggregates (sum / uniqMerge over
    # possibly unmerged partial rows), which is the required read pattern for
    # Summing/Aggregating targets. See docs/SCHEMA.md.
    ops = {}
    ops["volume"] = ensure_chart(
        api, "Volume per minute", ds_minute, "echarts_timeseries_bar", {
            "x_axis": "minute",
            "time_grain_sqla": "PT1M",
            "metrics": [metric_sql("sum(txn_count)", "Events")],
            "adhoc_filters": [sql_filter("minute > now() - INTERVAL 3 HOUR")],
            "row_limit": 10000,
        })
    ops["failure_rate"] = ensure_chart(
        api, "Failure rate per minute", ds_minute, "echarts_timeseries_line", {
            "x_axis": "minute",
            "time_grain_sqla": "PT1M",
            "metrics": [metric_sql("sum(failed_count) / sum(txn_count)", "Failure rate")],
            "adhoc_filters": [sql_filter("minute > now() - INTERVAL 3 HOUR")],
            "row_limit": 10000,
            "y_axis_format": ".1%",
        })
    ops["active"] = ensure_chart(
        api, "Active merchants per minute", ds_minute, "echarts_timeseries_line", {
            "x_axis": "minute",
            "time_grain_sqla": "PT1M",
            "metrics": [metric_sql("uniqMerge(active_merchants)", "Active merchants")],
            "adhoc_filters": [sql_filter("minute > now() - INTERVAL 3 HOUR")],
            "row_limit": 10000,
        })
    ops["top_today"] = ensure_chart(
        api, "Top merchants (last 24h)", ds_events, "table", {
            "query_mode": "aggregate",
            "groupby": ["merchant_id"],
            "metrics": [
                metric_sql("sumIf(amount, status = 'Success')", "Volume"),
                metric_sql("count(*)", "Events"),
                metric_sql("countIf(status = 'Failed') / count(*)", "Failure rate"),
            ],
            "adhoc_filters": [sql_filter("event_time > now() - INTERVAL 24 HOUR")],
            "row_limit": 10,
            "order_desc": True,
            "series_limit_metric": metric_sql("sumIf(amount, status = 'Success')", "Volume"),
            "column_config": {"Failure rate": {"d3NumberFormat": ".2%"},
                              "Volume": {"d3NumberFormat": ",.0f"}},
        })
    ops["anomalies"] = ensure_chart(
        api, "Anomaly watchlist", ds_anom, "table", {
            "query_mode": "raw",
            "all_columns": ["merchant_id", "industry", "txns_15m", "failed_15m",
                            "failure_rate_15m", "baseline_rate", "baseline_source",
                            "z_score"],
            "order_by_cols": ['["z_score", false]'],
            "row_limit": 50,
            "column_config": {"failure_rate_15m": {"d3NumberFormat": ".2%"},
                              "baseline_rate": {"d3NumberFormat": ".2%"}},
        })

    ops_id = ensure_dashboard(api, OPS_DASHBOARD, [
        [chart_component(ops["volume"], "Volume per minute", 6, 50),
         chart_component(ops["failure_rate"], "Failure rate per minute", 6, 50)],
        [chart_component(ops["active"], "Active merchants per minute", 6, 50),
         chart_component(ops["top_today"], "Top merchants (last 24h)", 6, 50)],
        [chart_component(ops["anomalies"], "Anomaly watchlist", 12, 40)],
    ], refresh_frequency=30)
    print(f"Operations dashboard assembled (id {ops_id})")

    # ---- History dashboard (rollups only, no auto refresh) ----
    hist = {}
    hist["monthly"] = ensure_chart(
        api, "Monthly transaction volume", ds_daily, "echarts_timeseries_bar", {
            "x_axis": "day",
            "time_grain_sqla": "P1M",
            "metrics": [metric_sql("sum(txn_count)", "Transactions")],
            "row_limit": 10000,
        })
    hist["daily_failures"] = ensure_chart(
        api, "Daily failed transactions", ds_daily, "echarts_timeseries_line", {
            "x_axis": "day",
            "time_grain_sqla": "P1D",
            "metrics": [metric_sql("sum(failed_count)", "Failed transactions")],
            "row_limit": 10000,
        })
    hist["seg_failure"] = ensure_chart(
        api, "Failure rate by merchant segment", ds_rollup_seg, "echarts_timeseries_bar", {
            "x_axis": "segment",
            "metrics": [metric_sql("sum(failed_count) / sum(txn_count)", "Failure rate")],
            "row_limit": 100,
            "y_axis_format": ".2%",
            "x_axis_sort": "Failure rate",
            "x_axis_sort_asc": False,
        })
    hist["top_all_time"] = ensure_chart(
        api, "Top merchants by lifetime volume", ds_daily, "table", {
            "query_mode": "aggregate",
            "groupby": ["merchant_id"],
            "metrics": [
                metric_sql("sum(success_amount)", "Lifetime volume"),
                metric_sql("sum(txn_count)", "Transactions"),
            ],
            "row_limit": 15,
            "order_desc": True,
            "series_limit_metric": metric_sql("sum(success_amount)", "Lifetime volume"),
            "column_config": {"Lifetime volume": {"d3NumberFormat": ",.0f"}},
        })
    hist["segments"] = ensure_chart(
        api, "Merchants per segment", ds_segments, "echarts_timeseries_bar", {
            "x_axis": "segment",
            "metrics": [metric_sql("count(*)", "Merchants")],
            "row_limit": 100,
            "x_axis_sort": "Merchants",
            "x_axis_sort_asc": False,
        })

    hist_id = ensure_dashboard(api, HISTORY_DASHBOARD, [
        [chart_component(hist["monthly"], "Monthly transaction volume", 6, 50),
         chart_component(hist["daily_failures"], "Daily failed transactions", 6, 50)],
        [chart_component(hist["seg_failure"], "Failure rate by merchant segment", 6, 50),
         chart_component(hist["top_all_time"], "Top merchants by lifetime volume", 6, 50)],
        [chart_component(hist["segments"], "Merchants per segment", 12, 45)],
    ])
    print(f"History dashboard assembled (id {hist_id})")

    # ---- System monitoring dashboard (the stack watching itself) ----
    ds_parts = ensure_dataset(api, db_id, "system_parts", sql=SYSTEM_PARTS_SQL)

    mon = {}
    mon["rate"] = ensure_chart(
        api, "Ingestion rate (rows per minute)", ds_minute, "echarts_timeseries_bar", {
            "x_axis": "minute",
            "time_grain_sqla": "PT1M",
            "metrics": [metric_sql("sum(txn_count)", "Rows ingested")],
            "adhoc_filters": [sql_filter("minute > now() - INTERVAL 60 MINUTE")],
            "row_limit": 10000,
        })
    # Lag = wall clock minus newest event in the raw table. Covers the whole
    # path: producer generating, buffering, inserting, part committed.
    mon["lag"] = ensure_chart(
        api, "Ingestion lag", ds_events, "big_number_total", {
            "metric": metric_sql("dateDiff('second', max(event_time), now())",
                                 "Seconds behind wall clock"),
            "subheader": "seconds between now() and max(event_time); healthy is under ~15s (flush interval + insert)",
        })
    mon["parts"] = ensure_chart(
        api, "Storage: active parts and size per table", ds_parts, "table", {
            "query_mode": "raw",
            "all_columns": ["table", "active_parts", "rows",
                            "compressed_mib", "uncompressed_mib"],
            "order_by_cols": ['["rows", false]'],
            "row_limit": 20,
        })

    mon_id = ensure_dashboard(api, MONITORING_DASHBOARD, [
        [chart_component(mon["rate"], "Ingestion rate (rows per minute)", 8, 50),
         chart_component(mon["lag"], "Ingestion lag", 4, 50)],
        [chart_component(mon["parts"], "Storage: active parts and size per table", 12, 40)],
    ], refresh_frequency=30)
    print(f"Monitoring dashboard assembled (id {mon_id})")

    os.makedirs(os.path.dirname(EXPORT_PATH), exist_ok=True)
    export = api.get("/dashboard/export/", params={"q": f"!({ops_id},{hist_id},{mon_id})"})
    with open(EXPORT_PATH, "wb") as f:
        f.write(export.content)
    print(f"Exported all dashboards to {os.path.relpath(EXPORT_PATH)} "
          f"({len(export.content):,} bytes)")
    print(f"Operations: {BASE}/superset/dashboard/{ops_id}/")
    print(f"History:    {BASE}/superset/dashboard/{hist_id}/")
    print(f"Monitoring: {BASE}/superset/dashboard/{mon_id}/")


if __name__ == "__main__":
    main()

