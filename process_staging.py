# Third-party
import glob
import hashlib
import json
import logging

# Stdlib
import os
import re
from datetime import datetime
from typing import Dict, List, Tuple

import duckdb
import pandas as pd

from logging_utils import configure_logging
from silver_transforms import run_silver_transforms
from gold_transforms import run_gold_transforms

DB_FILE = "nelo_analytics_rewrite.db"
STAGING_DIR = "staging"
PROCESSED_DIR = "processed"
LOG_FILE = "etl.log"


# Logging is configured via logging_utils.configure_logging


def validate_and_coerce(df: pd.DataFrame, logger) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Return (good_df, bad_records). bad_records contain {'stage':'validate','error':..., 'flat_record':..., 'raw_event':...}
    """
    required_cols = ["event_id", "event_name", "event_timestamp"]
    bad = []
    good_rows = []

    for idx, row in df.iterrows():
        errors = []

        # Required columns
        for col in required_cols:
            if pd.isna(row.get(col)):
                errors.append(f"missing_required:{col}")

        # event_timestamp must be a pandas Timestamp or convertible
        ts = row.get("event_timestamp")
        if ts is None or (
            not hasattr(ts, "value") and not isinstance(ts, pd.Timestamp)
        ):
            errors.append("invalid:event_timestamp")

        # event_id must be non-empty string
        if not row.get("event_id"):
            errors.append("invalid:event_id")

        if errors:
            bad.append(
                {
                    "stage": "validate",
                    "error": ",".join(errors),
                    "flat_record": {k: row.get(k, None) for k in df.columns},
                    "raw_event": row.get("raw_event"),
                }
            )
        else:
            good_rows.append(row)

    good_df = (
        pd.DataFrame(good_rows, columns=df.columns)
        if good_rows
        else pd.DataFrame(columns=df.columns)
    )
    logger.info(f"Validation result: good_rows={len(good_df)}, bad_rows={len(bad)}")
    return good_df, bad


def append_dead_letters(records: List[Dict], dlq_path: str):
    if not records:
        return
    os.makedirs(os.path.dirname(dlq_path), exist_ok=True)
    with open(dlq_path, "a") as dlq:
        for rec in records:
            dlq.write(json.dumps(rec, default=str) + "\n")


# --- Parsing Helpers -------------------------------------------------------------


def generate_event_id(event_timestamp, user_id, event_name, product_id):
    """
    Generate deterministic event ID for deduplication.

    Components: timestamp + user + event type + product + list context
    """
    # Convert timestamp to microseconds integer for consistency
    if hasattr(event_timestamp, "value"):
        ts_value = event_timestamp.value  # pandas Timestamp
    else:
        ts_value = int(event_timestamp.timestamp() * 1_000_000)

    composite = {
        "ts": str(ts_value),
        "uid": str(user_id),
        "evt": str(event_name),
        "pid": str(product_id or ""),
    }

    composite_str = json.dumps(composite, sort_keys=True)
    return hashlib.sha256(composite_str.encode()).hexdigest()


def parse_discount_codes(discount_str):
    """Extract campaign/experiment info from discount codes field in the item_params."""
    if not discount_str:
        # Return None so the column holds only NULL or list[str] (no empty Python lists)
        return {"campaigns": None, "experiment_variant": None}

    codes = discount_str.split("|")
    experiment_variant = None
    campaign_list = []

    for code in codes:
        # Check for experiment patterns (NELO_XP_XX format)
        if "NELO_XP_" in code:
            experiment_variant = code
        else:
            campaign_list.append(code)

    return {
        "campaigns": "|".join(campaign_list) if campaign_list else None,
        "experiment_variant": experiment_variant,
    }


# -----------------------------------------------------------------------------
# Simple, robust GA4 items parser using targeted extraction
# -----------------------------------------------------------------------------


def split_items_array(items_str: str) -> list:
    """
    Split the items array string into individual item strings.

    Handles nested structures by tracking bracket depth.
    Input: "[{item1...}, {item2...}]"
    Output: ["{item1...}", "{item2...}"]
    """
    # Remove outer brackets
    if items_str.startswith("["):
        items_str = items_str[1:]
    if items_str.endswith("]"):
        items_str = items_str[:-1]

    items = []
    current_item = []
    depth = 0  # Track {} and [] depth

    i = 0
    while i < len(items_str):
        char = items_str[i]

        if char == "{":
            depth += 1
            current_item.append(char)
        elif char == "}":
            depth -= 1
            current_item.append(char)
            # If we're back to depth 0, we've finished an item
            if depth == 0:
                # Look ahead for comma separator
                j = i + 1
                while j < len(items_str) and items_str[j] in " \t\n":
                    j += 1
                if j < len(items_str) and items_str[j] == ",":
                    i = j  # Skip the comma
                # Save this item
                items.append("".join(current_item))
                current_item = []
        elif char == "[":
            depth += 10  # Use different depth for square brackets to track them
            current_item.append(char)
        elif char == "]":
            depth -= 10
            current_item.append(char)
        else:
            if depth > 0:  # Only add chars when inside an item
                current_item.append(char)

        i += 1

    # Add any remaining item
    if current_item and "".join(current_item).strip():
        items.append("".join(current_item))

    return items


def parse_single_item(item_str: str) -> dict:
    """
    Extract fields from a single item string using targeted regex patterns.

    Input: "{item_id=xxx, item_name=yyy, ...}"
    Output: dict with all fields already extracted and flattened
    """
    result = {
        "item_id": None,
        "item_name": None,
        "item_list_id": None,
        "item_list_name": None,
        "price_in_usd": None,
        "price": None,
        "quantity": None,
        "item_revenue_in_usd": None,
        "item_revenue": None,
        # item_params fields (flattened)
        "discount_value": None,
        "total_price": None,
        "installments": None,
        "installment_price": None,
        "discounts": None,
        "is_in_stock": None,
    }

    # Top-level string fields
    for field in ["item_id", "item_name", "item_list_id", "item_list_name"]:
        # Replace the pattern line with:
        pattern = rf"{field}=([^,}}]+?)(?:\s*,|\s*(?:item_|price_|quantity|coupon|affiliation|location|promotion|creative|item_params))"
        match = re.search(pattern, item_str)
        if match:
            val = match.group(1).strip()
            if val not in ("(not set)", "null", ""):
                result[field] = val

    # Numeric top-level fields
    for field in [
        "price_in_usd",
        "price",
        "quantity",
        "item_revenue_in_usd",
        "item_revenue",
    ]:
        pattern = rf"{field}=([0-9.]+)"
        match = re.search(pattern, item_str)
        if match:
            val = match.group(1)
            result[field] = float(val) if "." in val else int(val)

    # item_params extraction - find the value for each known key
    param_keys = {
        "discountt": "discount_value",
        "totalPrice": "total_price",
        "number_of_installments": "installments",
        "installment_price": "installment_price",
        "discounts": "discounts",
        "in_stock": "is_in_stock",
    }

    for param_key, result_key in param_keys.items():
        # Try all possible value fields (double, float, int, string)
        for value_type in ["double_value", "float_value", "int_value", "string_value"]:
            pattern = rf"key={param_key},\s*value=\{{[^}}]*?{value_type}=([^,}}]+)"
            match = re.search(pattern, item_str)
            if match:
                val_str = match.group(1).strip()
                if val_str not in ("null", "(not set)", ""):
                    # Try to convert to number
                    clean_val = val_str.replace("$", "").replace(",", "").strip()
                    if re.match(r"^-?[0-9.]+$", clean_val):
                        result[result_key] = (
                            float(clean_val) if "." in clean_val else int(clean_val)
                        )
                    else:
                        result[result_key] = (
                            val_str  # Keep as string only if not numeric
                        )
                    break

    return result


def parse_items_array(items_str: str) -> list:
    """
    Parse the entire items array and return a list of item dicts.

    This is the main entry point - handles both single and multiple items.
    Returns a list of dicts with all fields already extracted and flattened.
    """
    if not items_str or items_str == "[]":
        return []

    # Split into individual items
    item_strings = split_items_array(items_str)

    # Parse each item
    results = []
    for item_str in item_strings:
        parsed_item = parse_single_item(item_str)
        results.append(parsed_item)

    return results


def process_events_to_df(events):
    """Processes a list of events and returns a flattened DataFrame."""
    records = []

    for event in events:
        items_data = event.get("items", [])

        # Parse items string if needed
        if isinstance(items_data, str):
            items_data = parse_items_array(items_data)

        if not items_data:
            continue

        # The parser already returns dicts with all fields extracted and flattened
        for item in items_data:

            discount_info = parse_discount_codes(item.get("discounts"))

            flat_record = {
                # Event-level fields
                "event_id": generate_event_id(
                    pd.to_datetime(event.get("event_timestamp"), unit="us"),
                    event.get("user_id"),
                    event.get("event_name"),
                    item.get("item_id"),
                ),
                "event_name": event.get("event_name"),
                "event_timestamp": pd.to_datetime(
                    event.get("event_timestamp"), unit="us"
                ),
                "replay_timestamp": pd.to_datetime(event.get("replay_timestamp")),
                "ingestion_timestamp": pd.to_datetime(datetime.now()),
                "user_id": event.get("user_id"),
                "platform": event.get("platform"),
                "raw_event": json.dumps(event),
                # Item-level fields (already extracted by parser)
                "item_list_id": item.get("item_list_id"),
                "item_list_name": item.get("item_list_name"),
                "product_id": item.get("item_id"),
                "product_name": item.get("item_name"),
                "price_in_usd": item.get("price_in_usd"),
                "price": item.get("price"),
                "quantity": item.get("quantity"),
                "item_revenue_in_usd": item.get("item_revenue_in_usd"),
                "item_revenue": item.get("item_revenue"),
                # item_params fields (already extracted by parser)
                "discount_value": item.get("discount_value"),
                "total_price": item.get("total_price"),
                "installments": item.get("installments"),
                "installment_price": item.get("installment_price"),
                "discount_campaigns": discount_info.get("campaigns"),
                "discount_experiment_variant": discount_info.get("experiment_variant"),
                "is_in_stock": item.get("is_in_stock"),
            }

            records.append(flat_record)

    df = pd.DataFrame(records)
    logging.info(f"Created DataFrame with {len(df)} rows from {len(events)} events")
    return df


def process_staging_file(filepath, con, run_ts, logger, fmt):
    """Processes a single staging files and loads into the DuckDB."""

    file_basename = os.path.basename(filepath).replace(".jsonl", "")
    per_file_log = f"logs/processing_{file_basename}_{run_ts}.log"
    file_fh = logging.FileHandler(per_file_log)
    file_fh.setFormatter(fmt)
    logger.addHandler(file_fh)

    dlq_path = f"dead_letter/process_staging_failures_{run_ts}.jsonl"
    started = datetime.now()
    logger.info(f"Processing staging file: {filepath}")

    try:
        total_lines = 0
        parsed_events = []
        parse_dead = []

        # PARSE (per-line so we don’t lose the batch; capture parse poison pills)
        with open(filepath, "r") as f:
            for i, line in enumerate(f, start=1):
                total_lines += 1
                try:
                    evt = json.loads(line)
                    parsed_events.append(evt)
                except Exception as e:
                    parse_dead.append(
                        {
                            "stage": "parse",
                            "filepath": filepath,
                            "line_number": i,
                            "error": str(e),
                            "raw_event": line.strip(),
                        }
                    )

        append_dead_letters(parse_dead, dlq_path)
        logger.info(
            f"Parsed lines={total_lines}, parsed_ok={len(parsed_events)}, parse_dead={len(parse_dead)}"
        )

        if not parsed_events:
            logger.warning(f"No valid events after parsing. Skipping file {filepath}.")
            return True

        # FLATTEN
        df = process_events_to_df(parsed_events)
        if df.empty:
            logging.info(f"No records in {filepath}. Skipping.")
            return True

        # Add processed_at column (will be set to NULL initially, updated by silver_transforms)
        df["processed_at"] = None

        # VALIDATE
        df_good, validate_dead = validate_and_coerce(df, logger)
        append_dead_letters(validate_dead, dlq_path)
        if df_good.empty:
            logger.warning(
                f"All rows failed validation for {filepath}. Nothing to insert."
            )
            return True

        # Ensure all staging columns are present in the correct order
        stg_columns = [
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

        # Select and reorder columns (all should exist now)
        df_good = df_good[stg_columns]

        # BULK INSERT (append-only, no deduplication at staging layer)
        inserted = 0
        try:
            con.register("df_to_load", df_good)
            con.execute(
                """
                INSERT INTO stg_events
                SELECT * FROM df_to_load
            """
            )
            con.unregister("df_to_load")
            inserted = len(df_good)
            logger.info(f"Bulk insert succeeded: rows={inserted}")
        except Exception as e:
            logger.error(f"Bulk insert failed, falling back to row-wise: {e}")
            # ROW-WISE FALLBACK to catch per-row insert poison pills
            row_dead = []
            insert_sql = f"""
                INSERT INTO stg_events ({", ".join(stg_columns)})
                VALUES ({", ".join(["?"]*len(stg_columns))})
            """
            for r_idx, row in df_good.iterrows():
                values = [row[c] for c in stg_columns]
                try:
                    con.execute(insert_sql, values)
                    inserted += 1
                except Exception as rexc:
                    row_dead.append(
                        {
                            "stage": "insert",
                            "error": str(rexc),
                            "flat_record": {c: row[c] for c in stg_columns},
                            "raw_event": row.get("raw_event"),
                        }
                    )
            append_dead_letters(row_dead, dlq_path)
            logger.info(
                f"Row-wise insert done: inserted={inserted}, insert_dead={len(row_dead)}"
            )

        took = (datetime.now() - started).total_seconds()
        logger.info(
            f"File done: {filepath} lines={total_lines} parsed_ok={len(parsed_events)} "
            f"flattened={len(df)} inserted={inserted} duration_s={took:.2f}"
        )
        return True

    except Exception as e:
        logger.error(f"Fatal error processing {filepath}: {e}", exc_info=True)
        return False
    finally:
        logger.removeHandler(file_fh)
        file_fh.close()


def main():
    run_ts = os.getenv("RUN_TS") or datetime.now().strftime("%Y%m%d_%H%M%S")
    logger, fmt = configure_logging(
        f"logs/process_staging_{run_ts}.log", logger_name="process_staging"
    )
    logger.info("--- Starting Staging Processing ---")
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # DuckDB connect with retries ONLY for transient file lock issues
    con = None
    last_err = None
    for attempt in range(3):
        try:
            con = duckdb.connect(database=DB_FILE, read_only=False)
            break
        except (IOError, OSError) as e:
            # Only retry IO errors: file locks, permissions, disk issues
            # Check if it's a retryable error (file lock conflicts)
            error_msg = str(e).lower()
            if "lock" in error_msg or "permission" in error_msg:
                last_err = e
                logger.warning(f"DuckDB connect failed (attempt {attempt+1}/3): {e}")
                if attempt < 2:  # Don't sleep on last attempt
                    import time

                    time.sleep(0.5 * (2**attempt))
            else:
                # Non-retryable IO error (e.g., file not found, disk full)
                logger.error(f"DuckDB connect failed with non-retryable IO error: {e}")
                return
        except Exception as e:
            # ALL other errors are non-retryable:
            # - SQL errors (BinderException, CatalogException, SyntaxError)
            # - Database corruption
            # - Schema issues
            # - Type errors, Value errors, etc.
            logger.error(
                f"DuckDB connect failed with non-retryable error: {type(e).__name__}: {e}"
            )
            return

    if con is None:
        logger.error(f"Failed to connect to DuckDB after retries: {last_err}")
        return

    staging_files = glob.glob(os.path.join(STAGING_DIR, "processing_batch_*.jsonl"))
    if not staging_files:
        logger.info("No files found in staging directory. Exiting.")
        return

    for filepath in staging_files:
        try:
            # --- Transaction 1: Staging and Silver ---
            con.begin()
            if process_staging_file(filepath, con, run_ts, logger, fmt):
                # Run Silver transforms within same transaction as staging insert
                logger.info("Running silver transforms...")
                run_silver_transforms(con, logger, session_minutes=30)
                logger.info("Silver transforms executed successfully.")

                # Commit staging and silver transforms together
                con.commit()
                logger.info("Staging and Silver transaction committed successfully.")

                # Move file to processed directory only after successful commit
                dest_path = os.path.join(PROCESSED_DIR, os.path.basename(filepath))
                os.rename(filepath, dest_path)
                logger.info(f"Moved {filepath} to {dest_path}")

            else:
                con.rollback()
                logger.warning(
                    f"Rolled back transaction for file {filepath}. File not moved."
                )
                continue  # Skip to the next file if staging/silver failed

            # --- Transaction 2: Gold ---
            # This runs outside the first transaction. If it fails, the Silver data is already safe.
            # try:
            #     logger.info("Running gold transforms in a separate transaction...")
            #     con.begin()
            #     run_gold_transforms(con, logger )
            #     con.commit()
            #     logger.info("Gold transforms transaction committed successfully.")
            # except Exception as gold_err:
            #     con.rollback()
            #     logger.error(
            #         f"Gold transforms failed for batch from {filepath}. Silver data is preserved. Error: {gold_err}",
            #         exc_info=True,
            #     )
            #     # This error will still propagate up and fail the run, which is desired.
            #     raise

        except Exception as e:
            try:
                con.rollback()
                logger.error(
                    f"Main transaction for {filepath} failed: {e}. Rolled back.",
                    exc_info=True,
                )
            except Exception as rollback_err:
                logger.error(f"Rollback also failed: {rollback_err}", exc_info=True)


    con.close()
    logger.info("--- Staging Processing Finished ---")


if __name__ == "__main__":
    main()
