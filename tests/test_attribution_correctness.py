# tests/test_attribution_correctness.py
import json
import logging
from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd
import pytest

import setup_database as db_setup
from silver_transforms import run_silver_transforms


@pytest.fixture()
def temp_duckdb(tmp_path):
    db_path = tmp_path / "test_attr.db"
    db_setup.DB_FILE = str(db_path)
    db_setup.setup_database()
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        yield con
    finally:
        con.close()


def _insert_stg(con: duckdb.DuckDBPyConnection, rows: list[dict]):
    df = pd.DataFrame(rows)
    con.register("_df", df)
    cols = [
        "event_id",
        "event_name",
        "event_timestamp",
        "replay_timestamp",
        "ingestion_timestamp",
        "user_id",
        "platform",
        "item_list_id",
        "item_list_name",
        "product_id",
        "product_name",
        "price_in_usd",
        "price",
        "quantity",
        "item_revenue_in_usd",
        "item_revenue",
        "discount_value",
        "total_price",
        "discount_campaigns",
        "discount_experiment_variant",
        "installments",
        "installment_price",
        "is_in_stock",
        "raw_event",
        "processed_at",
    ]
    select_expr = ", ".join(
        [
            f"_df.{c}"
            if c != "raw_event"
            else "CAST(_df.raw_event AS JSON) AS raw_event"
            for c in cols
        ]
    )
    con.execute(
        f"INSERT INTO stg_events ({', '.join(cols)}) SELECT {select_expr} FROM _df"
    )
    con.unregister("_df")


def _evt(eid, name, ts, user, list_id=None):
    return {
        "event_id": eid,
        "event_name": name,
        "event_timestamp": pd.Timestamp(ts),
        "replay_timestamp": pd.Timestamp(ts),
        "ingestion_timestamp": pd.Timestamp(ts),
        "user_id": user,
        "platform": "IOS",
        "item_list_id": list_id,
        "item_list_name": f"List-{list_id}" if list_id else None,
        "product_id": "P1",
        "product_name": "Prod",
        "price_in_usd": 1.0,
        "price": 1.0,
        "quantity": 1,
        "item_revenue_in_usd": 1.0,
        "item_revenue": 1.0 if name == "purchase" else 0.0,
        "discount_value": None,
        "total_price": 1.0,
        "discount_campaigns": None,
        "discount_experiment_variant": None,
        "installments": 0,
        "installment_price": None,
        "is_in_stock": True,
        "raw_event": json.dumps({"k": "v"}),
        "processed_at": None,
    }


def test_attribution_within_30_minutes(temp_duckdb):
    con = temp_duckdb
    base = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

    rows = [
        _evt("i1", "view_item_list", base, "u1", "L1"),
        _evt("v1", "view_item", base + timedelta(minutes=10), "u1"),
        _evt("c1", "begin_checkout", base + timedelta(minutes=20), "u1"),
        _evt("p1", "purchase", base + timedelta(minutes=25), "u1"),
        _evt("i2", "view_item_list", base + timedelta(minutes=40), "u1", "L2"),
        _evt("v2", "view_item", base + timedelta(minutes=50), "u1"),
        _evt("p2", "purchase", base + timedelta(minutes=55), "u1"),
        _evt(
            "p3", "purchase", base + timedelta(hours=2), "u1"
        ),  # outside window → no attribution
    ]
    _insert_stg(con, rows)
    run_silver_transforms(con, logging.getLogger("silver"), session_minutes=30)

    # Only events with list attribution inside 30 min should be present
    atts = con.execute(
        """
        SELECT event_id, event_name, item_list_id
        FROM silver_event_list_attribution
        ORDER BY event_id
        """
    ).fetchall()

    # Expect v1,c1,p1 attributed to L1; v2,p2 to L2; p3 absent (no attribution)
    got = {(e, n, l) for (e, n, l) in atts}
    expected = {
        ("v1", "view_item", "L1"),
        ("c1", "begin_checkout", "L1"),
        ("p1", "purchase", "L1"),
        ("v2", "view_item", "L2"),
        ("p2", "purchase", "L2"),
    }
    assert expected.issubset(got)
    assert not any(eid == "p3" for (eid, _, _) in atts)
