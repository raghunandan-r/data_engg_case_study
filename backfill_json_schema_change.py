"""
Generic backfill script to extract fields from raw_event column.

This script can backfill multiple fields at once by parsing the raw_event JSON
and extracting values from the GA4 items string.

USAGE:
------
# Dry run (shows what would be updated, no changes to database):
python backfill_json_schema_change.py

# Live mode (actually updates the database):
python backfill_json_schema_change.py --live

# Specify which fields to backfill (comma-separated):
python backfill_json_schema_change.py --fields product_category,coupon,promotion_id

SUPPORTED FIELDS:
-----------------
- product_category_1: Product category level 1 (from item_category)
- product_category_2: Product category level 2 (from item_category2)
- product_category_3: Product category level 3 (from item_category3)
- product_category_4: Product category level 4 (from item_category4)
- product_category_5: Product category level 5 (from item_category5)
- promotion_id: GA4 promotion ID
- promotion_name: GA4 promotion name
- creative_name: GA4 creative name
- creative_slot: GA4 creative slot
- coupon: Coupon code
- affiliation: Affiliation value

NOTES:
------
- By default runs in DRY RUN mode - safe to test
- Processes rows in batches of 1000 for efficiency
- Logs all actions to backfill.log
- Only updates rows where target field is currently NULL
- Handles "(not set)" and "null" as NULL values

EXPECTED OUTCOME WITH CURRENT DATA:
-----------------------------------
Since most fields are currently "(not set)" in the source, this script will:
- Process all rows with NULL target fields
- Find 0 or very few populated values
- Update 0 or very few rows
- Confirm that NULL is correct for unpopulated fields

This script becomes useful if/when GA4 tracking starts populating these fields.
"""

import json
import logging
import re
from typing import Optional

import duckdb

# Configuration
DB_FILE = "nelo_growth_analytics.db"
LOG_FILE = "backfill.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

# Field configs: Maps source fields (GA4 items string) to target columns
FIELD_MAPPINGS = {
    "product_category_1": {
        "source_fields": ["item_category"],
        "target_column": "product_category_1",
        "description": "Product category level 1 (top level)",
        "extractor": "simple",
    },
    "product_category_2": {
        "source_fields": ["item_category2"],
        "target_column": "product_category_2",
        "description": "Product category level 2",
        "extractor": "simple",
    },
    "product_category_3": {
        "source_fields": ["item_category3"],
        "target_column": "product_category_3",
        "description": "Product category level 3",
        "extractor": "simple",
    },
    "product_category_4": {
        "source_fields": ["item_category4"],
        "target_column": "product_category_4",
        "description": "Product category level 4",
        "extractor": "simple",
    },
    "product_category_5": {
        "source_fields": ["item_category5"],
        "target_column": "product_category_5",
        "description": "Product category level 5 (most specific)",
        "extractor": "simple",
    },
    "promotion_id": {
        "source_fields": ["promotion_id"],
        "target_column": "promotion_id",
        "description": "GA4 promotion ID",
        "extractor": "simple",
    },
    "promotion_name": {
        "source_fields": ["promotion_name"],
        "target_column": "promotion_name",
        "description": "GA4 promotion name",
        "extractor": "simple",
    },
    "creative_name": {
        "source_fields": ["creative_name"],
        "target_column": "creative_name",
        "description": "GA4 creative name",
        "extractor": "simple",
    },
    "creative_slot": {
        "source_fields": ["creative_slot"],
        "target_column": "creative_slot",
        "description": "GA4 creative slot",
        "extractor": "simple",
    },
    "coupon": {
        "source_fields": ["coupon"],
        "target_column": "coupon",
        "description": "Coupon code",
        "extractor": "simple",
    },
    "affiliation": {
        "source_fields": ["affiliation"],
        "target_column": "affiliation",
        "description": "Affiliation value",
        "extractor": "simple",
    },
}


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


def extract_simple_field(item_str: str, field_name: str) -> Optional[str]:
    """
    Extract a single field value from the GA4 items string.

    Input: "{item_id=..., field_name=value, ...}"
    Output: Field value, or None if "(not set)" or null
    """
    # Pattern to extract the field value
    # Matches until comma or end of string, but stops before another field= pattern
    pattern = rf"{field_name}=([^,\]]+?)(?:,|\s*(?:item_|price_|quantity|coupon|affiliation|location|promotion|creative|item_params))"
    match = re.search(pattern, item_str)

    if match:
        val = match.group(1).strip()
        # Return value only if it's not a null placeholder
        if val not in ("(not set)", "null", "", "None"):
            logging.debug(f"Found {field_name}={val}")
            return val

    return None


def extract_field_from_item(item_str: str, field_config: dict) -> Optional[str]:
    """
    Extract field value from the GA4 item string based on configuration.

    Args:
        item_str: The GA4 item string
        field_config: Configuration dict with 'extractor' and 'source_fields'

    Returns:
        Extracted value or None
    """
    extractor_type = field_config["extractor"]
    source_fields = field_config["source_fields"]

    if extractor_type == "simple":
        # For simple fields, extract the first (and only) source field
        return extract_simple_field(item_str, source_fields[0])
    else:
        logging.error(f"Unknown extractor type: {extractor_type}")
        return None


def extract_fields_from_raw_event(
    raw_event_json: str, field_configs: dict, row_index: int = 0
) -> dict:
    """
    Extract multiple fields from a raw_event JSON string.

    Args:
        raw_event_json: JSON string from raw_event column
        field_configs: Dict of field_name -> field_config mappings
        row_index: Which item index to extract (default 0 for first item)

    Returns:
        Dict of field_name -> extracted_value (None if not found)
    """
    results = {field_name: None for field_name in field_configs.keys()}

    try:
        # Parse the raw_event JSON
        event_data = json.loads(raw_event_json)

        # Get the items string
        items_str = event_data.get("items", "")
        if not items_str or items_str == "[]":
            return results

        # Split into individual items
        item_strings = split_items_array(items_str)

        # Get the requested item (default first item)
        if row_index >= len(item_strings):
            logging.warning(
                f"Row index {row_index} out of range for {len(item_strings)} items"
            )
            return results

        item_str = item_strings[row_index]

        # Extract each configured field
        for field_name, field_config in field_configs.items():
            value = extract_field_from_item(item_str, field_config)
            results[field_name] = value

        return results

    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse raw_event JSON: {e}")
        return results
    except Exception as e:
        logging.error(f"Error extracting fields: {e}")
        return results


def backfill_fields(
    fields_to_backfill: list = None, dry_run: bool = True, batch_size: int = 1000
):
    """
    Backfill multiple fields from raw_event data.

    Args:
        fields_to_backfill: List of field names to backfill (from FIELD_MAPPINGS keys)
                           If None, defaults to all 5 product category levels
        dry_run: If True, only show what would be updated without committing
        batch_size: Number of rows to process per batch
    """
    # Default to all 5 category levels if not specified
    if fields_to_backfill is None:
        fields_to_backfill = [
            "product_category_1",
            "product_category_2",
            "product_category_3",
            "product_category_4",
            "product_category_5",
        ]

    # Validate requested fields
    invalid_fields = [f for f in fields_to_backfill if f not in FIELD_MAPPINGS]
    if invalid_fields:
        logging.error(f"Invalid field names: {invalid_fields}")
        logging.error(f"Available fields: {list(FIELD_MAPPINGS.keys())}")
        return

    # Get configurations for requested fields
    field_configs = {f: FIELD_MAPPINGS[f] for f in fields_to_backfill}

    logging.info("=" * 80)
    logging.info("Starting Field Backfill")
    logging.info("=" * 80)
    logging.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")
    logging.info(f"Database: {DB_FILE}")
    logging.info(f"Batch size: {batch_size}")
    logging.info(f"Fields to backfill: {', '.join(fields_to_backfill)}")
    for field_name in fields_to_backfill:
        logging.info(f"  - {field_name}: {FIELD_MAPPINGS[field_name]['description']}")

    try:
        # Connect to DuckDB
        con = duckdb.connect(database=DB_FILE, read_only=dry_run)
        logging.info("Connected to database")

        # Count total rows
        total_rows = con.execute("SELECT COUNT(*) FROM stg_events").fetchone()[0]
        logging.info(f"Total rows in stg_events: {total_rows}")

        # Build WHERE clause for rows that need backfilling
        # (at least one target field is NULL)
        target_columns = [
            FIELD_MAPPINGS[f]["target_column"] for f in fields_to_backfill
        ]
        where_conditions = [f"{col} IS NULL" for col in target_columns]
        where_clause = " OR ".join(where_conditions)

        # Count rows where at least one target field is NULL
        null_count = con.execute(
            f"""
            SELECT COUNT(*)
            FROM stg_events
            WHERE {where_clause}
        """
        ).fetchone()[0]
        logging.info(f"Rows needing backfill: {null_count}")

        if null_count == 0:
            logging.info("No rows to backfill. Exiting.")
            con.close()
            return

        # Track statistics per field
        field_stats = {f: {"found": 0, "updated": 0} for f in fields_to_backfill}

        # Fetch rows in batches
        offset = 0
        total_rows_processed = 0

        while offset < null_count:
            logging.info(
                f"\nProcessing batch {offset // batch_size + 1} (rows {offset} to {min(offset + batch_size, null_count)})..."
            )

            # Build SELECT with all target columns to check current values
            select_cols = ["event_id", "raw_event"] + target_columns

            # Fetch batch
            rows = con.execute(
                f"""
                SELECT {', '.join(select_cols)}
                FROM stg_events
                WHERE {where_clause}
                LIMIT {batch_size} OFFSET {offset}
            """
            ).fetchall()

            if not rows:
                break

            # Process each row
            updates_by_row = []  # List of (event_id, {field: value})

            for row in rows:
                event_id = row[0]
                raw_event = row[1]
                current_values = {
                    target_columns[i]: row[i + 2] for i in range(len(target_columns))
                }

                # Extract all requested fields from raw_event
                extracted = extract_fields_from_raw_event(raw_event, field_configs)

                # Determine which fields need updating (NULL and value found)
                updates_for_row = {}
                for field_name in fields_to_backfill:
                    target_col = FIELD_MAPPINGS[field_name]["target_column"]
                    extracted_value = extracted[field_name]

                    if (
                        current_values[target_col] is None
                        and extracted_value is not None
                    ):
                        updates_for_row[target_col] = extracted_value
                        field_stats[field_name]["found"] += 1
                        logging.debug(
                            f"  Event {event_id}: {field_name} = '{extracted_value}'"
                        )

                if updates_for_row:
                    updates_by_row.append((event_id, updates_for_row))

            # Apply updates if not dry run
            if updates_by_row and not dry_run:
                con.begin()
                for event_id, field_updates in updates_by_row:
                    # Build dynamic UPDATE statement
                    set_clauses = [f"{col} = ?" for col in field_updates.keys()]
                    set_sql = ", ".join(set_clauses)
                    values = list(field_updates.values()) + [event_id]

                    con.execute(
                        f"""
                        UPDATE stg_events
                        SET {set_sql}
                        WHERE event_id = ?
                    """,
                        values,
                    )

                    # Update per-field statistics
                    for col in field_updates.keys():
                        for field_name in fields_to_backfill:
                            if FIELD_MAPPINGS[field_name]["target_column"] == col:
                                field_stats[field_name]["updated"] += 1

                con.commit()
                logging.info(f"  Updated {len(updates_by_row)} rows in this batch")
            elif updates_by_row:
                # Dry run: count what would be updated
                for _, field_updates in updates_by_row:
                    for col in field_updates.keys():
                        for field_name in fields_to_backfill:
                            if FIELD_MAPPINGS[field_name]["target_column"] == col:
                                field_stats[field_name]["updated"] += 1
                logging.info(
                    f"  [DRY RUN] Would update {len(updates_by_row)} rows in this batch"
                )
            else:
                logging.info("No values found in this batch")

            total_rows_processed += len(rows)
            offset += batch_size

        # Summary
        logging.info("\n" + "=" * 80)
        logging.info("BACKFILL SUMMARY")
        logging.info("=" * 80)
        logging.info(f"Total rows processed: {total_rows_processed}")
        logging.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")
        logging.info("")

        for field_name in fields_to_backfill:
            stats = field_stats[field_name]
            logging.info(f"{field_name}:")
            logging.info(f"  - Values found: {stats['found']}")
            logging.info(
                f"  - Rows {'would be ' if dry_run else ''}updated: {stats['updated']}"
            )

        if all(stats["found"] == 0 for stats in field_stats.values()):
            logging.info("\n⚠️  No values found for any field in raw_event data")
            logging.info(
                "This is expected if all fields are currently '(not set)' in the source"
            )

        con.close()
        logging.info("\nBackfill complete!")

    except Exception as e:
        logging.error(f"Fatal error during backfill: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    import argparse
    import sys

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Backfill fields in stg_events from raw_event JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run for all 5 category levels (default)
  python backfill_json_schema_change.py

  # Live update for all 5 category levels
  python backfill_json_schema_change.py --live

  # Dry run for specific category levels
  python backfill_json_schema_change.py --fields product_category_1,product_category_2

  # Backfill promotional fields only
  python backfill_json_schema_change.py --fields promotion_id,promotion_name,creative_name,creative_slot

  # Backfill categories + coupon
  python backfill_json_schema_change.py --live --fields product_category_1,product_category_2,product_category_3,product_category_4,product_category_5,coupon

  # Backfill all available fields
  python backfill_json_schema_change.py --fields all
        """,
    )

    parser.add_argument(
        "--live", action="store_true", help="Execute live updates (default is dry run)"
    )

    parser.add_argument(
        "--fields",
        type=str,
        default="product_category_1,product_category_2,product_category_3,product_category_4,product_category_5",
        help=f'Comma-separated list of fields to backfill. Use "all" for all fields. Available: {", ".join(FIELD_MAPPINGS.keys())}',
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows to process per batch (default: 1000)",
    )

    args = parser.parse_args()

    # Parse fields argument
    if args.fields.lower() == "all":
        fields_to_backfill = list(FIELD_MAPPINGS.keys())
    else:
        fields_to_backfill = [f.strip() for f in args.fields.split(",")]

    # Warn about live mode
    if not args.live:
        logging.info("Running in DRY RUN mode. Use --live to commit changes.")
    else:
        logging.warning("⚠️  LIVE MODE: Changes will be committed to the database!")
        logging.warning("Press Ctrl+C within 3 seconds to cancel...")
        import time

        try:
            time.sleep(3)
        except KeyboardInterrupt:
            logging.info("\nCancelled by user.")
            sys.exit(0)

    # Run backfill
    backfill_fields(
        fields_to_backfill=fields_to_backfill,
        dry_run=not args.live,
        batch_size=args.batch_size,
    )
