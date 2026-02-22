#!/usr/bin/env python3
"""
Lakehouse Dashboard Provisioner for Apache Superset.

Creates a "Sales Lakehouse" dashboard with eight charts automatically:
  Row 1 — KPI Summary:
    1. Total Revenue (Big Number)
    2. Total Orders (Big Number)
    3. Average Order Value (Big Number)
  Row 2 — Geographic & Distribution:
    4. Revenue by Country (Bar Chart)
    5. Orders by Country (Pie Chart)
  Row 3 — Trends:
    6. Daily Revenue Trend (Line Chart)
    7. Daily Orders Trend (Line Chart)
  Row 4 — Customer Detail:
    8. Top 10 Customers (Table)

Idempotent: skips creation if the dashboard already exists.

Usage:
    python3 /app/provision_dashboard.py

Requires environment variables:
    SUPERSET_ADMIN_USERNAME  (default: admin)
    SUPERSET_ADMIN_PASSWORD  (default: admin)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time

import requests

logging.basicConfig(level=logging.INFO, format="[dashboard-provisioner] %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "http://localhost:8088"
USERNAME = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

# Global session — maintains cookies (required for CSRF)
session = requests.Session()

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def login() -> None:
    """Login and configure the session with auth + CSRF headers."""
    resp = session.post(
        f"{BASE_URL}/api/v1/security/login",
        json={"username": USERNAME, "password": PASSWORD, "provider": "db", "refresh": True},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]

    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Referer": BASE_URL,
    })

    # Fetch CSRF token (session cookies are set automatically)
    csrf_resp = session.get(f"{BASE_URL}/api/v1/security/csrf_token/", timeout=10)
    csrf_resp.raise_for_status()
    csrf_token = csrf_resp.json()["result"]

    session.headers.update({
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json",
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_database_id() -> int | None:
    """Find the 'Trino Lakehouse' database ID."""
    resp = session.get(f"{BASE_URL}/api/v1/database/", timeout=10)
    resp.raise_for_status()
    for db in resp.json().get("result", []):
        if db["database_name"] == "Trino Lakehouse":
            return db["id"]
    return None


def get_or_create_dataset(database_id: int) -> int:
    """Get or create the 'sales' dataset and return its ID."""
    resp = session.get(
        f"{BASE_URL}/api/v1/dataset/",
        params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": "sales"}]})},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json().get("result", [])
    if results:
        log.info("Dataset 'sales' already exists (id=%d)", results[0]["id"])
        return results[0]["id"]

    payload = {
        "database": database_id,
        "table_name": "sales",
        "schema": "lakehouse",
        "catalog": "iceberg",
    }
    resp = session.post(f"{BASE_URL}/api/v1/dataset/", json=payload, timeout=15)
    resp.raise_for_status()
    dataset_id = resp.json()["id"]
    log.info("Created dataset 'sales' (id=%d)", dataset_id)
    return dataset_id


def dashboard_exists(title: str) -> bool:
    """Check if a dashboard with this title already exists."""
    resp = session.get(
        f"{BASE_URL}/api/v1/dashboard/",
        params={"q": json.dumps({"filters": [{"col": "dashboard_title", "opr": "eq", "value": title}]})},
        timeout=10,
    )
    resp.raise_for_status()
    return len(resp.json().get("result", [])) > 0


def create_chart(dataset_id: int, chart_def: dict) -> int:
    """Create a chart and return its ID."""
    resp = session.get(
        f"{BASE_URL}/api/v1/chart/",
        params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": chart_def["slice_name"]}]})},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json().get("result", [])
    if results:
        log.info("Chart '%s' already exists (id=%d)", chart_def["slice_name"], results[0]["id"])
        return results[0]["id"]

    payload = {
        "slice_name": chart_def["slice_name"],
        "viz_type": chart_def["viz_type"],
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": json.dumps(chart_def["params"]),
    }
    resp = session.post(f"{BASE_URL}/api/v1/chart/", json=payload, timeout=15)
    resp.raise_for_status()
    chart_id = resp.json()["id"]
    log.info("Created chart '%s' (id=%d)", chart_def["slice_name"], chart_id)
    return chart_id


# ---------------------------------------------------------------------------
# Chart Definitions
# ---------------------------------------------------------------------------

CHARTS = [
    {
        "slug": "total_revenue",
        "slice_name": "Total Revenue",
        "viz_type": "big_number_total",
        "params": {
            "datasource": None,  # filled at runtime
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "SUM(amount)",
                "label": "Total Revenue",
            },
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "$,.2f",
        },
    },
    {
        "slug": "revenue_by_country",
        "slice_name": "Revenue by Country",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "datasource": None,
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "country",
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "SUM(amount)",
                    "label": "Revenue",
                }
            ],
            "groupby": [],
            "order_desc": True,
            "y_axis_format": "$,.2f",
            "show_legend": False,
            "rich_tooltip": True,
        },
    },
    {
        "slug": "daily_revenue",
        "slice_name": "Daily Revenue Trend",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "datasource": None,
            "viz_type": "echarts_timeseries_line",
            "x_axis": "ingestion_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "SUM(amount)",
                    "label": "Daily Revenue",
                }
            ],
            "y_axis_format": "$,.2f",
            "rich_tooltip": True,
            "show_legend": False,
        },
    },
    {
        "slug": "orders_by_country",
        "slice_name": "Orders by Country",
        "viz_type": "pie",
        "params": {
            "datasource": None,
            "viz_type": "pie",
            "groupby": ["country"],
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Order Count",
            },
            "show_labels": True,
            "show_legend": True,
            "label_type": "key_percent",
            "number_format": "SMART_NUMBER",
        },
    },
    {
        "slug": "total_orders",
        "slice_name": "Total Orders",
        "viz_type": "big_number_total",
        "params": {
            "datasource": None,
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Total Orders",
            },
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
        },
    },
    {
        "slug": "avg_order_value",
        "slice_name": "Average Order Value",
        "viz_type": "big_number_total",
        "params": {
            "datasource": None,
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "AVG(amount)",
                "label": "Avg Order Value",
            },
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "$,.2f",
        },
    },
    {
        "slug": "daily_orders",
        "slice_name": "Daily Orders Trend",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "datasource": None,
            "viz_type": "echarts_timeseries_line",
            "x_axis": "ingestion_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Daily Orders",
                }
            ],
            "rich_tooltip": True,
            "show_legend": False,
        },
    },
    {
        "slug": "top_customers",
        "slice_name": "Top 10 Customers",
        "viz_type": "table",
        "params": {
            "datasource": None,
            "viz_type": "table",
            "groupby": ["customer_id"],
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "SUM(amount)",
                    "label": "Total Revenue",
                },
                {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Order Count",
                },
            ],
            "order_desc": True,
            "row_limit": 10,
            "include_time": False,
            "table_timestamp_format": "smart_date",
        },
    },
]

# ---------------------------------------------------------------------------
# Dashboard layout
# ---------------------------------------------------------------------------


def build_dashboard_layout(chart_ids: dict[str, int]) -> dict:
    """Build a Superset dashboard position dict with a 4-row grid.

    Row 1: 3 KPIs (width 4 each)
    Row 2: Revenue by Country + Orders by Country (width 6 each)
    Row 3: Daily Revenue Trend + Daily Orders Trend (width 6 each)
    Row 4: Top 10 Customers table (width 12, full width)
    """
    return {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": ["ROW-1", "ROW-2", "ROW-3", "ROW-4"],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": "Sales Lakehouse Dashboard"},
        },
        # Row 1: KPI Summary (3 x width-4)
        "ROW-1": {
            "type": "ROW",
            "id": "ROW-1",
            "children": ["CHART-1", "CHART-2", "CHART-3"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "CHART-1": {
            "type": "CHART",
            "id": "CHART-1",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
            "meta": {
                "width": 4,
                "height": 30,
                "chartId": chart_ids["total_revenue"],
                "sliceName": "Total Revenue",
            },
        },
        "CHART-2": {
            "type": "CHART",
            "id": "CHART-2",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
            "meta": {
                "width": 4,
                "height": 30,
                "chartId": chart_ids["total_orders"],
                "sliceName": "Total Orders",
            },
        },
        "CHART-3": {
            "type": "CHART",
            "id": "CHART-3",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
            "meta": {
                "width": 4,
                "height": 30,
                "chartId": chart_ids["avg_order_value"],
                "sliceName": "Average Order Value",
            },
        },
        # Row 2: Geographic & Distribution (2 x width-6)
        "ROW-2": {
            "type": "ROW",
            "id": "ROW-2",
            "children": ["CHART-4", "CHART-5"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "CHART-4": {
            "type": "CHART",
            "id": "CHART-4",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW-2"],
            "meta": {
                "width": 6,
                "height": 50,
                "chartId": chart_ids["revenue_by_country"],
                "sliceName": "Revenue by Country",
            },
        },
        "CHART-5": {
            "type": "CHART",
            "id": "CHART-5",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW-2"],
            "meta": {
                "width": 6,
                "height": 50,
                "chartId": chart_ids["orders_by_country"],
                "sliceName": "Orders by Country",
            },
        },
        # Row 3: Trends (2 x width-6)
        "ROW-3": {
            "type": "ROW",
            "id": "ROW-3",
            "children": ["CHART-6", "CHART-7"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "CHART-6": {
            "type": "CHART",
            "id": "CHART-6",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW-3"],
            "meta": {
                "width": 6,
                "height": 50,
                "chartId": chart_ids["daily_revenue"],
                "sliceName": "Daily Revenue Trend",
            },
        },
        "CHART-7": {
            "type": "CHART",
            "id": "CHART-7",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW-3"],
            "meta": {
                "width": 6,
                "height": 50,
                "chartId": chart_ids["daily_orders"],
                "sliceName": "Daily Orders Trend",
            },
        },
        # Row 4: Customer Detail (1 x width-12)
        "ROW-4": {
            "type": "ROW",
            "id": "ROW-4",
            "children": ["CHART-8"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "CHART-8": {
            "type": "CHART",
            "id": "CHART-8",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW-4"],
            "meta": {
                "width": 12,
                "height": 50,
                "chartId": chart_ids["top_customers"],
                "sliceName": "Top 10 Customers",
            },
        },
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    dashboard_title = "Sales Lakehouse Dashboard"

    log.info("Starting dashboard provisioning ...")

    # Wait for Superset API to be ready
    for attempt in range(1, 31):
        try:
            resp = session.get(f"{BASE_URL}/health", timeout=5)
            if resp.status_code == 200:
                break
        except requests.ConnectionError:
            pass
        log.info("Waiting for Superset API (attempt %d/30) ...", attempt)
        time.sleep(2)
    else:
        log.error("Superset API not available after 60s. Skipping dashboard provisioning.")
        return

    login()

    # Check if dashboard already exists
    if dashboard_exists(dashboard_title):
        log.info("Dashboard '%s' already exists. Skipping.", dashboard_title)
        return

    # Get database ID
    db_id = get_database_id()
    if db_id is None:
        log.error("Trino Lakehouse database not found in Superset. Skipping.")
        return
    log.info("Found Trino Lakehouse database (id=%d)", db_id)

    # Create dataset
    dataset_id = get_or_create_dataset(db_id)

    # Create charts
    chart_ids: dict[str, int] = {}
    for chart_def in CHARTS:
        chart_def["params"]["datasource"] = f"{dataset_id}__table"
        chart_id = create_chart(dataset_id, chart_def)
        chart_ids[chart_def["slug"]] = chart_id

    # Create dashboard
    position = build_dashboard_layout(chart_ids)
    dashboard_payload = {
        "dashboard_title": dashboard_title,
        "published": True,
        "position_json": json.dumps(position),
        "slug": "sales-lakehouse",
    }
    resp = session.post(
        f"{BASE_URL}/api/v1/dashboard/",
        json=dashboard_payload,
        timeout=15,
    )
    resp.raise_for_status()
    dash_id = resp.json()["id"]
    log.info("Created dashboard '%s' (id=%d)", dashboard_title, dash_id)

    # Link charts to dashboard via PUT json_metadata with positions.
    # Superset's set_dash_metadata reads "positions" from json_metadata to
    # populate the dashboard_slices association table.
    metadata = {
        "positions": position,
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "refresh_frequency": 0,
        "default_filters": "{}",
        "color_scheme": "",
        "label_colors": {},
        "shared_label_colors": {},
        "color_scheme_domain": [],
    }
    resp = session.put(
        f"{BASE_URL}/api/v1/dashboard/{dash_id}",
        json={"json_metadata": json.dumps(metadata)},
        timeout=15,
    )
    resp.raise_for_status()
    log.info("Linked %d charts to dashboard", len(chart_ids))

    log.info("Dashboard provisioning complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Dashboard provisioning failed (non-fatal)")
        sys.exit(0)  # Non-fatal: Superset still starts
