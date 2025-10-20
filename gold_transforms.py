import logging
import os
from datetime import datetime

import duckdb

from logging_utils import configure_logging

DB_FILE = "nelo_analytics_rewrite.db"


def run_gold_transforms(con: duckdb.DuckDBPyConnection, logger: logging.Logger):
    """
    Main function to run all gold layer transformations.
    Connects to DuckDB and executes the transform functions.
    """
    logger.info("--- Starting Gold Layer Transformations ---")
    try:
        create_gold_product_campaign_semantic_views(con, logger)

        logger.info("--- Gold Layer Transformations Completed Successfully ---")

    except Exception as e:
        logger.error(f"An error occurred during gold transformations: {e}", exc_info=True)
        raise  # Re-raise to allow transaction rollback in the caller


def create_gold_product_campaign_semantic_views(con, logger):
    logger.info("Creating gold product campaign semantic views...")

    # 1) Price change events during active campaigns (observed pairs only if fct_product_campaigns exists)
    con.execute(
        """
        CREATE OR REPLACE VIEW vw_gold_product_campaign_price_change_events AS
        WITH changes AS (
            SELECT
                product_id,
                LAG(price) OVER (PARTITION BY product_id ORDER BY effective_start_date) AS old_price,
                price AS new_price,
                effective_start_date AS change_date
            FROM dim_products
        )
        SELECT
            g.campaign_id,
            c.campaign_name,
            ch.product_id,
            ch.change_date,
            ch.old_price,
            ch.new_price,
            (ch.new_price - ch.old_price) / NULLIF(ch.old_price, 0) AS delta_pct
        FROM changes ch
        JOIN dim_campaigns c
          ON c.effective_start_date <= ch.change_date
         AND (c.effective_end_date IS NULL OR ch.change_date < c.effective_end_date)
        JOIN fct_product_campaigns g
          ON g.product_id = ch.product_id
         AND g.campaign_id = c.campaign_id
         AND g.first_seen_date <= ch.change_date
         AND ch.change_date <= g.last_seen_date
        WHERE ch.old_price IS NOT NULL AND ch.new_price IS DISTINCT FROM ch.old_price;
        """
    )

    # 2) Current status: active campaign x active product with current price and discount flags
    # Uses fct_product_campaigns (small) instead of scanning fct_events.
    con.execute(
        """
        CREATE OR REPLACE VIEW vw_gold_product_campaign_current_status AS
        WITH current_products AS (
            SELECT product_id, price
            FROM dim_products
            WHERE is_active = TRUE
        ),
        current_campaigns AS (
            SELECT campaign_id, campaign_name
            FROM dim_campaigns
            WHERE is_active = TRUE
        )
        SELECT
            c.campaign_id,
            c.campaign_name,
            p.product_id,
            p.price AS current_price,            
            /* has_discount_today: observed combos whose last_seen_date is today and discount_value > 0 */
            MAX(CASE WHEN f.discount_value > 0 AND f.last_seen_date = CURRENT_DATE THEN 1 ELSE 0 END) AS has_discount_today,
            /* has_discount_recent: any observed discount historically while dims are currently active */
            MAX(CASE WHEN f.discount_value > 0 THEN 1 ELSE 0 END) AS has_discount_recent,
            COALESCE(MAX(CASE WHEN f.last_seen_date = CURRENT_DATE THEN f.discount_value / NULLIF(p.price, 0) * 100 END), 0) AS current_discount_percentage
        FROM current_campaigns c
        JOIN fct_product_campaigns f
          ON f.campaign_id = c.campaign_id
        JOIN current_products p
          ON p.product_id = f.product_id
        GROUP BY c.campaign_id, c.campaign_name, p.product_id, p.price;
        """
    )

    logger.info("Gold product campaign semantic views created.")



if __name__ == "__main__":
    run_ts = os.getenv("RUN_TS") or datetime.now().strftime("%Y%m%d_%H%M%S")
    logger, _ = configure_logging(
        f"logs/gold_transforms_{run_ts}.log", logger_name="gold_transforms"
    )

    con = None
    try:
        con = duckdb.connect(database=DB_FILE, read_only=False)
        logger.info("Successfully connected to DuckDB for standalone run.")

        con.begin()
        run_gold_transforms(con, logger)
        con.commit()
        logger.info("Standalone gold transforms committed.")

    except Exception as e:
        logger.error(f"Standalone gold transformations run failed: {e}", exc_info=True)
        if con:
            con.rollback()
            logger.info("Transaction rolled back.")
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
