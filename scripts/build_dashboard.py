"""
Builds the Superset dashboard through the REST API, then exports it.

Why a script instead of clicking: the dashboard definition becomes code that
can be reviewed in a pull request and rebuilt from scratch, and the exported
zip it produces (superset/exports/) is the import artifact for anyone cloning
the repo. Run it with the stack up and the data loaded:

    py -3.11 scripts/build_dashboard.py

Idempotent: reuses the database connection and replaces charts, datasets and
the dashboard if they already exist (matched by name).
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
DASHBOARD_TITLE = "Payments Portfolio Overview"
EXPORT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "superset", "exports", "payments_portfolio_dashboard.zip",
)

SEGMENT_DATASET_SQL = """\
SELECT
    t.transaction_id,
    t.merchant_id,
    t.transaction_date,
    t.amount,
    t.status,
    s.segment
FROM payments.transactions AS t
INNER JOIN payments.merchant_segments AS s USING (merchant_id)
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
    result = api.post("/database/", {
        "database_name": DB_NAME,
        "sqlalchemy_uri": f"clickhousedb://analytics:{CLICKHOUSE_PASSWORD}@clickhouse:8123/payments",
        "expose_in_sqllab": True,
    })
    return result["id"]


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
    query = {
        "columns": columns,
        "filters": [],
        "row_limit": params.get("row_limit", 10000),
        "extras": {"having": "", "where": ""},
    }
    if params.get("metrics"):
        query["metrics"] = params["metrics"]
    if params.get("query_mode") == "raw" and params.get("order_by_cols"):
        query["orderby"] = [json.loads(o) for o in params["order_by_cols"]]
    if params.get("time_grain_sqla"):
        query["extras"]["time_grain_sqla"] = params["time_grain_sqla"]
    if params.get("series_limit_metric"):
        query["orderby"] = [[params["series_limit_metric"], not params.get("order_desc", True)]]
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


def build_position_json(rows):
    """rows: list of lists of chart components (one inner list per dashboard row)."""
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
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


def main():
    api = Superset()

    db_id = ensure_database(api)
    print(f"Database connection ready (id {db_id})")

    ds_txn = ensure_dataset(api, db_id, "transactions")
    ds_daily = ensure_dataset(api, db_id, "daily_merchant_stats")
    ds_seg = ensure_dataset(api, db_id, "transactions_with_segments", sql=SEGMENT_DATASET_SQL)
    ds_anom = ensure_dataset(api, db_id, "merchant_anomalies")
    print(f"Datasets ready: transactions={ds_txn}, daily_stats={ds_daily}, "
          f"segments={ds_seg}, anomalies={ds_anom}")

    charts = {}

    charts["volume"] = ensure_chart(
        api, "Transaction volume over time", ds_txn, "echarts_timeseries_bar", {
            "x_axis": "transaction_date",
            "time_grain_sqla": "P1M",
            "metrics": [metric_sql("count(*)", "Transactions")],
            "row_limit": 10000,
            "y_axis_title": "Transactions per month",
        })

    charts["segments"] = ensure_chart(
        api, "Failure rate by merchant segment", ds_seg, "echarts_timeseries_bar", {
            "x_axis": "segment",
            "metrics": [metric_sql(
                "countIf(status = 'Failed') / count(*)", "Failure rate")],
            "row_limit": 100,
            "y_axis_format": ".2%",
            "x_axis_sort": "Failure rate",
            "x_axis_sort_asc": False,
        })

    charts["top_merchants"] = ensure_chart(
        api, "Top merchants by processed volume", ds_txn, "table", {
            "query_mode": "aggregate",
            "groupby": ["merchant_id"],
            "metrics": [
                metric_sql("sumIf(amount, status = 'Success')", "Processed volume"),
                metric_sql("count(*)", "Transactions"),
            ],
            "row_limit": 15,
            "order_desc": True,
            "series_limit_metric": metric_sql(
                "sumIf(amount, status = 'Success')", "Processed volume"),
            "column_config": {"Processed volume": {"d3NumberFormat": ",.2f"}},
        })

    # Reads the SummingMergeTree pre-aggregate, and aggregates again at query
    # time (sum over possibly unmerged partial rows), which is the correct way
    # to read an MV target. See docs/SCHEMA.md.
    charts["daily_failures"] = ensure_chart(
        api, "Daily failed transactions (from materialized view)", ds_daily,
        "echarts_timeseries_line", {
            "x_axis": "day",
            "time_grain_sqla": "P1D",
            "metrics": [metric_sql("sum(failed_count)", "Failed transactions")],
            "row_limit": 10000,
        })

    # The view computes and filters everything itself, so the chart is a raw
    # read: whatever rows the view returns ARE the watchlist.
    charts["anomalies"] = ensure_chart(
        api, "Anomaly watchlist (failure rate vs merchant baseline)", ds_anom, "table", {
            "query_mode": "raw",
            "all_columns": ["merchant_id", "industry", "txns_15m", "failed_15m",
                            "failure_rate_15m", "baseline_rate", "baseline_source",
                            "z_score"],
            "order_by_cols": ['["z_score", false]'],
            "row_limit": 50,
            "column_config": {
                "failure_rate_15m": {"d3NumberFormat": ".2%"},
                "baseline_rate": {"d3NumberFormat": ".2%"},
            },
        })

    print(f"Charts ready: {charts}")

    dash_id = api.find_one("dashboard", "dashboard_title", DASHBOARD_TITLE)
    if dash_id:
        api.delete(f"/dashboard/{dash_id}")

    position = build_position_json([
        [chart_component(charts["volume"], "Transaction volume over time", 6, 50),
         chart_component(charts["daily_failures"], "Daily failed transactions", 6, 50)],
        [chart_component(charts["segments"], "Failure rate by merchant segment", 6, 50),
         chart_component(charts["top_merchants"], "Top merchants by processed volume", 6, 50)],
        [chart_component(charts["anomalies"], "Anomaly watchlist", 12, 40)],
    ])
    dash_id = api.post("/dashboard/", {
        "dashboard_title": DASHBOARD_TITLE,
        "position_json": json.dumps(position),
        "published": True,
    })["id"]

    for chart_id in charts.values():
        api.put(f"/chart/{chart_id}", {"dashboards": [dash_id]})
    print(f"Dashboard assembled (id {dash_id})")

    os.makedirs(os.path.dirname(EXPORT_PATH), exist_ok=True)
    export = api.get("/dashboard/export/", params={"q": f"!({dash_id})"})
    with open(EXPORT_PATH, "wb") as f:
        f.write(export.content)
    print(f"Exported to {os.path.relpath(EXPORT_PATH)} ({len(export.content):,} bytes)")
    print(f"Open {BASE}/superset/dashboard/{dash_id}/ to view it.")


if __name__ == "__main__":
    main()
