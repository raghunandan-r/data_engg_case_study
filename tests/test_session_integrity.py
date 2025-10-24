# tests/test_session_integrity.py
"""
Test session integrity: ensure no double-counting, correct cross-batch updates,
and proper handling of prehistory in sessionization.
"""
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
    """Create a fresh test database."""
    db_path = tmp_path / "test_sessions.db"
    db_setup.DB_FILE = str(db_path)
    db_setup.setup_database()
    con = duckdb.connect(str(db_path), read_only=False)

    # Initialize ETL checkpoint (silver_transforms expects this now)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_checkpoints (
            pipeline VARCHAR PRIMARY KEY,
            hwm_ts TIMESTAMP NOT NULL,
            hwm_event_id VARCHAR NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO etl_checkpoints (pipeline, hwm_ts, hwm_event_id)
        VALUES ('silver_events', TIMESTAMP '1970-01-01', '')
        ON CONFLICT (pipeline) DO NOTHING;
    """
    )
    con.commit()
    try:
        yield con
    finally:
        con.close()


def _insert_stg(con: duckdb.DuckDBPyConnection, rows: list[dict]):
    """Insert rows into stg_events."""
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
    con.commit()


def _evt(
    eid,
    name,
    ts,
    user,
    platform="ANDROID",
    list_id=None,
    prod_id="prod_01TEST",
    price=100.0,
    revenue=0.0,
):
    """Create a test event matching the JSONL structure."""
    raw = {
        "event_timestamp": int(ts.timestamp() * 1_000_000),
        "user_id": user,
        "event_name": name,
        "platform": platform,
        "items": [
            {
                "item_id": prod_id,
                "item_name": f"Product {prod_id}",
                "item_list_id": list_id,
                "item_list_name": f"List {list_id}" if list_id else None,
                "price": price,
                "quantity": 1,
                "item_revenue": revenue if name == "purchase" else None,
            }
        ],
        "replay_timestamp": ts.isoformat(),
    }

    return {
        "event_id": eid,
        "event_name": name,
        "event_timestamp": pd.Timestamp(ts),
        "replay_timestamp": pd.Timestamp(ts),
        "ingestion_timestamp": pd.Timestamp(ts),
        "user_id": user,
        "platform": platform.upper(),
        "item_list_id": list_id,
        "item_list_name": f"List {list_id}" if list_id else None,
        "product_id": prod_id,
        "product_name": f"Product {prod_id}",
        "price_in_usd": price,
        "price": price,
        "quantity": 1,
        "item_revenue_in_usd": revenue if name == "purchase" else None,
        "item_revenue": revenue if name == "purchase" else None,
        "discount_value": None,
        "total_price": price,
        "discount_campaigns": None,
        "discount_experiment_variant": None,
        "installments": 0,
        "installment_price": None,
        "is_in_stock": True,
        "raw_event": json.dumps(raw),
    }


def test_no_duplicate_events_in_sessionization(temp_duckdb):
    """
    Test that events from current batch aren't duplicated when prehistory is loaded.
    Previously, events_window would union net_new with fct_events, causing duplication.
    """
    con = temp_duckdb
    base = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)

    # Batch 1: establish prehistory
    batch1 = [
        _evt("e1", "view_item_list", base, "user_a", list_id="list_1"),
        _evt("e2", "view_item", base + timedelta(minutes=5), "user_a"),
    ]
    _insert_stg(con, batch1)

    # Debug: check staging
    stg_count = con.execute(
        "SELECT COUNT(*) FROM stg_events WHERE user_id = 'user_a'"
    ).fetchone()[0]
    print(f"\nDEBUG after insert: stg_events count = {stg_count}")

    # Check HWM before transform
    hwm = con.execute(
        "SELECT hwm_ts, hwm_event_id FROM etl_checkpoints WHERE pipeline = 'silver_events'"
    ).fetchone()
    print(f"DEBUG HWM before: {hwm}")

    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    # Check platform resolution
    platforms = con.execute(
        """
        SELECT fe.event_id, fe.user_id, fe.platform_id, p.platform_name 
        FROM fct_events fe 
        LEFT JOIN dim_platforms p USING (platform_id)
        WHERE fe.user_id = 'user_a'
    """
    ).fetchall()
    print(f"DEBUG platform resolution: {platforms}")

    # Check HWM after transform
    hwm_after = con.execute(
        "SELECT hwm_ts, hwm_event_id FROM etl_checkpoints WHERE pipeline = 'silver_events'"
    ).fetchone()
    print(f"DEBUG HWM after: {hwm_after}")

    # Verify batch 1 created 1 session with 2 events
    sess1 = con.execute(
        """
        SELECT events_in_session, views
        FROM silver_user_sessions
        WHERE user_id = 'user_a'
    """
    ).fetchone()
    assert sess1 == (2, 1), f"Expected (2, 1), got {sess1}"

    # Batch 2: same session continues (within 30 min window)
    batch2 = [
        _evt("e3", "begin_checkout", base + timedelta(minutes=20), "user_a"),
        _evt("e4", "purchase", base + timedelta(minutes=25), "user_a", revenue=100.0),
    ]
    _insert_stg(con, batch2)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    # Check that session was UPDATED (not duplicated) and has correct total: 4 events
    sessions = con.execute(
        """
        SELECT user_id, session_start_ts, events_in_session, views, checkouts, purchases
        FROM silver_user_sessions
        WHERE user_id = 'user_a'
        ORDER BY session_start_ts
    """
    ).fetchall()

    assert len(sessions) == 1, f"Expected 1 session, found {len(sessions)}: {sessions}"

    user, start, evt_count, views, checkouts, purchases = sessions[0]
    assert evt_count == 4, f"Expected 4 events in session, got {evt_count}"
    assert views == 1, f"Expected 1 view, got {views}"
    assert checkouts == 1, f"Expected 1 checkout, got {checkouts}"
    assert purchases == 1, f"Expected 1 purchase, got {purchases}"

    # Verify fct_events has exactly 4 distinct events (no duplicates)
    fact_count = con.execute(
        """
        SELECT COUNT(DISTINCT event_id)
        FROM fct_events
        WHERE user_id = 'user_a'
    """
    ).fetchone()[0]
    assert fact_count == 4, f"Expected 4 events in fct_events, got {fact_count}"


def test_cross_batch_session_updates_end_time(temp_duckdb):
    """
    Test that sessions spanning multiple batches correctly update session_end_ts.
    """
    con = temp_duckdb
    base = datetime(2025, 1, 1, 14, 0, tzinfo=timezone.utc)

    # Batch 1: session starts
    batch1 = [
        _evt("e1", "view_item_list", base, "user_b", list_id="list_2"),
        _evt("e2", "view_item", base + timedelta(minutes=2), "user_b"),
    ]
    _insert_stg(con, batch1)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    sess1 = con.execute(
        """
        SELECT session_start_ts, session_end_ts, events_in_session
        FROM silver_user_sessions
        WHERE user_id = 'user_b'
    """
    ).fetchone()
    start1, end1, count1 = sess1
    assert count1 == 2
    assert (end1 - start1) == timedelta(
        minutes=2
    ), f"Expected end-start 2 min, got {end1 - start1}"

    # Batch 2: session extends (within 30 min)
    batch2 = [
        _evt("e3", "begin_checkout", base + timedelta(minutes=15), "user_b"),
    ]
    _insert_stg(con, batch2)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    sess2 = con.execute(
        """
        SELECT session_start_ts, session_end_ts, events_in_session
        FROM silver_user_sessions
        WHERE user_id = 'user_b'
    """
    ).fetchone()
    start2, end2, count2 = sess2

    # Session start should remain the same, end should update, count should increment
    assert start2 == start1, f"Session start changed: {start1} -> {start2}"
    assert (end2 - start2) == timedelta(
        minutes=15
    ), f"Expected end-start 15 min, got {end2 - start2}"
    assert count2 == 3, f"Expected 3 events, got {count2}"


def test_new_session_after_inactivity_window(temp_duckdb):
    """
    Test that events beyond the session window create a NEW session (not extend existing).
    """
    con = temp_duckdb
    base = datetime(2025, 1, 1, 16, 0, tzinfo=timezone.utc)

    # Batch 1: first session
    batch1 = [
        _evt("e1", "view_item_list", base, "user_c", list_id="list_3"),
        _evt("e2", "view_item", base + timedelta(minutes=5), "user_c"),
    ]
    _insert_stg(con, batch1)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    # Batch 2: event after 40 minutes (beyond 30 min window) -> new session
    batch2 = [
        _evt(
            "e3",
            "view_item_list",
            base + timedelta(minutes=40),
            "user_c",
            list_id="list_4",
        ),
        _evt("e4", "purchase", base + timedelta(minutes=42), "user_c", revenue=200.0),
    ]
    _insert_stg(con, batch2)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    sessions = con.execute(
        """
        SELECT session_start_ts, events_in_session, purchases
        FROM silver_user_sessions
        WHERE user_id = 'user_c'
        ORDER BY session_start_ts
    """
    ).fetchall()

    assert len(sessions) == 2, f"Expected 2 sessions, got {len(sessions)}: {sessions}"

    # First session: 2 events, 0 purchases
    assert sessions[0][1] == 2, f"Session 1 expected 2 events, got {sessions[0][1]}"
    assert sessions[0][2] == 0, f"Session 1 expected 0 purchases, got {sessions[0][2]}"

    # Second session: 2 events, 1 purchase
    assert sessions[1][1] == 2, f"Session 2 expected 2 events, got {sessions[1][1]}"
    assert sessions[1][2] == 1, f"Session 2 expected 1 purchase, got {sessions[1][2]}"


def test_multi_user_no_cross_contamination(temp_duckdb):
    """
    Test that sessionization with prehistory doesn't mix events across different users.
    """
    con = temp_duckdb
    base = datetime(2025, 1, 1, 18, 0, tzinfo=timezone.utc)

    # Batch 1: two users, overlapping times
    batch1 = [
        _evt("e1", "view_item", base, "user_d"),
        _evt("e2", "view_item", base + timedelta(seconds=10), "user_e"),
    ]
    _insert_stg(con, batch1)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    # Batch 2: both users continue
    batch2 = [
        _evt("e3", "purchase", base + timedelta(minutes=10), "user_d", revenue=100.0),
        _evt(
            "e4",
            "purchase",
            base + timedelta(minutes=10, seconds=5),
            "user_e",
            revenue=150.0,
        ),
    ]
    _insert_stg(con, batch2)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    sessions = con.execute(
        """
        SELECT user_id, events_in_session, purchases
        FROM silver_user_sessions
        ORDER BY user_id
    """
    ).fetchall()

    assert len(sessions) == 2, f"Expected 2 sessions (1 per user), got {len(sessions)}"

    # Each user should have exactly 2 events and 1 purchase in their session
    for user_id, evt_count, purch_count in sessions:
        assert evt_count == 2, f"User {user_id}: expected 2 events, got {evt_count}"
        assert (
            purch_count == 1
        ), f"User {user_id}: expected 1 purchase, got {purch_count}"


def test_identical_timestamps_deterministic_ordering(temp_duckdb):
    """
    Test that events with identical timestamps are handled deterministically.
    This tests the ORDER BY event_timestamp, event_id pattern.
    """
    con = temp_duckdb
    base = datetime(2025, 1, 1, 20, 0, tzinfo=timezone.utc)

    # Insert 3 events at the EXACT same timestamp (realistic with high-throughput systems)
    batch = [
        _evt("e1", "view_item_list", base, "user_f", list_id="list_5"),
        _evt("e2", "view_item", base, "user_f"),  # same timestamp
        _evt("e3", "begin_checkout", base, "user_f"),  # same timestamp
    ]
    _insert_stg(con, batch)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    # Should create 1 session with all 3 events
    sess = con.execute(
        """
        SELECT events_in_session, views, checkouts
        FROM silver_user_sessions
        WHERE user_id = 'user_f'
    """
    ).fetchone()

    assert sess == (3, 1, 1), f"Expected (3, 1, 1), got {sess}"

    # Run again with same data (simulates reprocessing/duplicates caught by event_id PK)
    # Should be idempotent - no change
    _insert_stg(con, batch)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    sess2 = con.execute(
        """
        SELECT events_in_session, views, checkouts
        FROM silver_user_sessions
        WHERE user_id = 'user_f'
    """
    ).fetchone()

    assert sess2 == sess, f"Idempotency failed: {sess} -> {sess2}"


def test_empty_batch_no_errors(temp_duckdb):
    """
    Test that running silver transforms on an empty batch doesn't error out.
    """
    con = temp_duckdb

    # Run with no staging data
    try:
        run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)
    except Exception as e:
        pytest.fail(f"Empty batch caused error: {e}")

    # Should have no sessions
    count = con.execute("SELECT COUNT(*) FROM silver_user_sessions").fetchone()[0]
    assert count == 0, f"Expected 0 sessions on empty batch, got {count}"


def test_platform_isolation(temp_duckdb):
    """
    Test that sessions are partitioned by (user_id, platform_id).
    Same user on different platforms should have separate sessions.
    """
    con = temp_duckdb
    base = datetime(2025, 1, 1, 22, 0, tzinfo=timezone.utc)

    batch = [
        _evt("e1", "view_item", base, "user_g", platform="IOS"),
        _evt(
            "e2",
            "purchase",
            base + timedelta(minutes=5),
            "user_g",
            platform="IOS",
            revenue=100.0,
        ),
        _evt(
            "e3", "view_item", base + timedelta(minutes=2), "user_g", platform="ANDROID"
        ),
        _evt(
            "e4",
            "purchase",
            base + timedelta(minutes=7),
            "user_g",
            platform="ANDROID",
            revenue=200.0,
        ),
    ]
    _insert_stg(con, batch)
    run_silver_transforms(con, logging.getLogger("test"), session_minutes=30)

    sessions = con.execute(
        """
        SELECT p.platform_name, s.events_in_session, s.purchases
        FROM silver_user_sessions s
        JOIN dim_platforms p USING (platform_id)
        WHERE s.user_id = 'user_g'
        ORDER BY p.platform_name
    """
    ).fetchall()

    assert (
        len(sessions) == 2
    ), f"Expected 2 sessions (1 per platform), got {len(sessions)}"

    # Android: 2 events, 1 purchase
    assert sessions[0] == ("ANDROID", 2, 1), f"Android session wrong: {sessions[0]}"

    # IOS: 2 events, 1 purchase
    assert sessions[1] == ("IOS", 2, 1), f"IOS session wrong: {sessions[1]}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
