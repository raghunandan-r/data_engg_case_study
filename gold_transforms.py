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


    # Session-based daily list view (no fct_events scan; exposures from silver_list_impressions, actions and revenue from silver_event_list_attribution):
    con.execute(
    """
        CREATE OR REPLACE VIEW vw_growth_engagement_daily AS
        WITH sessions AS (
        SELECT
            user_id,
            platform_id,
            user_id || ':' || CAST(platform_id AS varchar) || ':' ||
            CAST(session_start_ts AS varchar) AS session_id,
            session_date               -- ← canonical date for the whole session
        FROM silver_session_features
        ),
        -- 1. Impressions (one row per session-list)
        exposures AS (
        SELECT
            li.user_id,
            li.platform_id,
            li.session_id,
            li.item_list_id,
            COUNT(*)                      AS impressions,
            1                             AS session_exposed
        FROM silver_list_impressions li
        GROUP BY 1,2,3,4
        ),
        -- 2. Flags from the attributed events (view_item / checkout / purchase)
        flags AS (
        SELECT
            a.user_id,
            a.platform_id,
            a.session_id,
            a.item_list_id,
            MAX(CASE WHEN a.event_name = 'view_item'      THEN 1 ELSE 0 END) AS had_click,
            MAX(CASE WHEN a.event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS had_checkout,
            MAX(CASE WHEN a.event_name = 'purchase'       THEN 1 ELSE 0 END) AS had_purchase
        FROM silver_event_list_attribution a
        GROUP BY 1,2,3,4
        ),
        -- 3. Bring the two sides together on (session,list) — note: NO date in the join
        joined AS (
        SELECT
            s.session_date                                 AS date,   -- single truth-date
            COALESCE(e.platform_id, f.platform_id)         AS platform_id,
            COALESCE(e.user_id,     f.user_id)             AS user_id,
            COALESCE(e.session_id,  f.session_id)          AS session_id,
            COALESCE(e.item_list_id,f.item_list_id)        AS item_list_id,
            COALESCE(e.impressions, 0)                     AS impressions,
            COALESCE(e.session_exposed, 0)                 AS session_exposed,
            COALESCE(f.had_click, 0)                       AS had_click,
            COALESCE(f.had_checkout, 0)                    AS had_checkout,
            COALESCE(f.had_purchase, 0)                    AS had_purchase
        FROM exposures e
        FULL OUTER JOIN flags f
                ON e.user_id      = f.user_id
                AND e.platform_id  = f.platform_id
                AND e.session_id   = f.session_id
                AND e.item_list_id = f.item_list_id        -- keep list in the key!
        LEFT JOIN sessions s
                ON s.user_id      = COALESCE(e.user_id, f.user_id)
            AND s.platform_id  = COALESCE(e.platform_id, f.platform_id)
            AND s.session_id   = COALESCE(e.session_id, f.session_id)
        ),
        -- 4. Add campaign (optional helper) and aggregate
        with_campaign AS (
        SELECT
            j.date,
            j.platform_id,
            COALESCE(sc.campaign_id, 'NONE') AS campaign_id,
            j.item_list_id,
            j.session_id,
            j.session_exposed,
            j.impressions,
            j.had_click,
            j.had_checkout,
            j.had_purchase
        FROM joined j
        LEFT JOIN silver_session_campaign_last_touch sc
                ON sc.user_id      = j.user_id
                AND sc.platform_id  = j.platform_id
                AND sc.session_id   = j.session_id
        )
        SELECT
        date,
        platform_id,
        campaign_id,
        item_list_id,
        COUNT(DISTINCT session_id)                    AS sessions,
        SUM(session_exposed)                          AS sessions_exposed,
        SUM(impressions)                              AS list_impressions,
        SUM(had_click)                                AS sessions_with_click,
        SUM(had_checkout)                             AS sessions_with_checkout,
        SUM(had_purchase)                             AS sessions_with_purchase,
        SUM(had_click)::decimal
            / NULLIF(SUM(session_exposed), 0)         AS ctr_sessions,
        SUM(had_purchase)::decimal
            / NULLIF(SUM(session_exposed), 0)         AS purchase_rate_on_exposure,
        SUM(had_purchase)::decimal
            / NULLIF(SUM(had_click), 0)               AS purchase_rate_on_click
        FROM with_campaign
        GROUP BY date, platform_id, campaign_id, item_list_id;
    """
    )

    # Use silver_session_features for funnel and silver_purchases for revenue/installments.
    con.execute(
    """
        WITH ud AS (
        SELECT 
            user_id,
            region, 
            customer_segment, 
            risk_segment,
            effective_start_date, 
            effective_end_date
        FROM dim_users
        )
        SELECT
            p.event_date,
            ud.region,
            ud.customer_segment,
            ud.risk_segment,
            COUNT(*) AS purchases,
            SUM(COALESCE(p.total_price, 0)) AS total_original_price,
            SUM(COALESCE(p.item_revenue, 0)) AS total_revenue,
            SUM(COALESCE(p.discount_value, 0)) AS total_discount_value,
            SUM(CASE WHEN COALESCE(p.installments, 0) > 0 THEN 1 ELSE 0 END) AS purchases_with_installments,
            SUM(COALESCE(p.installment_price, 0))  AS total_installment_value,
            (SUM(COALESCE(p.item_revenue, 0)) / NULLIF(COUNT(*), 0)) AS avg_order_value
        FROM silver_purchases p
            JOIN ud
                ON ud.user_id = p.user_id
                AND ud.effective_start_date <= p.event_date
                AND (ud.effective_end_date IS NULL OR p.event_date < ud.effective_end_date)
        GROUP BY p.event_date, ud.region, ud.customer_segment, ud.risk_segment;
    """
    )

    # Segment: users who abandoned a cart but then bought a different product later.
    con.execute(
    """
        CREATE OR REPLACE VIEW gold_segment_abandon_then_bought_different_v AS
        WITH abandoned AS (
        SELECT user_id, platform_id, session_start_ts, session_end_ts, cart_products
        FROM silver_session_features
        WHERE is_abandoned_checkout is true
        ),
        later_purchases AS (
        SELECT user_id, platform_id, product_id, event_timestamp
        FROM silver_purchases
        )
        SELECT DISTINCT a.user_id, a.platform_id, a.session_start_ts
        FROM abandoned a
        JOIN later_purchases p
        ON p.user_id = a.user_id
        AND p.platform_id = a.platform_id
        AND p.event_timestamp > a.session_end_ts
        LEFT JOIN UNNEST(a.cart_products) cp(product_id_in_cart)
        ON cp.product_id_in_cart = p.product_id
        WHERE cp.product_id_in_cart IS NULL;              
    """
    )

    # Segments: users who bounced, abandoned a checkout, or abandoned a checkout with a high value.
    con.execute(
    """
        -- 1) Bounce
        CREATE OR REPLACE VIEW gold_segment_bounce_v AS
        SELECT *
        FROM silver_session_features
        WHERE is_bounce;

        -- 2) Checkout abandon
        CREATE OR REPLACE VIEW gold_segment_checkout_abandon_v AS
        SELECT *
        FROM silver_session_features
        WHERE is_abandoned_checkout;

        -- 3) High-value checkout abandon (> $500)
        CREATE OR REPLACE VIEW gold_segment_high_value_abandon_v AS
        SELECT *
        FROM silver_session_features
        WHERE is_high_value_abandoned_checkout;
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
