import logging

import duckdb

DB_FILE = "nelo_growth_analytics.db"


def run_silver_transforms(
    con: duckdb.DuckDBPyConnection, logger: logging.Logger
) -> None:
    """
    Run idempotent Silver-layer transforms:
      - Upsert dim_users, dim_products, dim_item_lists, dim_campaigns, dim_experiments
      - Insert into fct_events with ON CONFLICT do nothing
      - Update stg_events.processed_at for rows included in this run
    Pre-conditions: stg_events contains newly inserted batch rows.
    """

    # First, get all unprocessed staging rows into a temp view for this run
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW current_batch AS
        SELECT * FROM stg_events WHERE processed_at IS NULL;
    """
    )

    # Dedupe within batch, then anti-join to facts to get net-new
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW net_new AS
        WITH batch_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY ingestion_timestamp DESC
                ) AS rn
            FROM current_batch
        ),
        deduped AS (
            SELECT * FROM batch_dedup WHERE rn = 1
        )
        SELECT d.*
        FROM deduped d
        LEFT JOIN fct_events f
            ON f.event_id = d.event_id
        WHERE f.event_id IS NULL;
    """
    )

    # Users
    logger.info("Upserting into dim_users...")
    con.execute(
        """
        INSERT INTO dim_users (
            user_id,
            first_seen_timestamp,
            last_seen_timestamp,
            acquisition_week,
            acquisition_month,
            total_events,
            total_purchases,
            total_revenue,
            first_purchase_timestamp,
            has_used_installments,
            total_view_items,
            total_checkouts,
            days_since_acquisition,
            lifetime_value,
            updated_at
        )
        SELECT
            user_id,
            MIN(event_timestamp) AS first_seen_timestamp,
            MAX(event_timestamp) AS last_seen_timestamp,
            CAST(DATE_TRUNC('week', MIN(event_timestamp)) AS DATE) AS acquisition_week,
            CAST(DATE_TRUNC('month', MIN(event_timestamp)) AS DATE) AS acquisition_month,
            COUNT(*) AS total_events,
            COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS total_purchases,
            SUM(CASE WHEN event_name = 'purchase' THEN item_revenue ELSE 0 END) AS total_revenue,
            MIN(CASE WHEN event_name = 'purchase' THEN event_timestamp END) AS first_purchase_timestamp,
            BOOL_OR(event_name = 'purchase' AND installments > 0) AS has_used_installments,
            COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) AS total_view_items,
            COUNT(CASE WHEN event_name = 'begin_checkout' THEN 1 END) AS total_checkouts,
            today() - DATE(MIN(event_timestamp)) AS days_since_acquisition,
            SUM(CASE WHEN event_name = 'purchase' THEN item_revenue ELSE 0 END) AS lifetime_value,
            now() AS updated_at
        FROM net_new
        WHERE user_id IS NOT NULL
        GROUP BY user_id
        ON CONFLICT (user_id) DO UPDATE SET
            last_seen_timestamp = GREATEST(dim_users.last_seen_timestamp, EXCLUDED.last_seen_timestamp),
            acquisition_week = LEAST(dim_users.acquisition_week, EXCLUDED.acquisition_week),
            acquisition_month = LEAST(dim_users.acquisition_month, EXCLUDED.acquisition_month),
            total_events = dim_users.total_events + EXCLUDED.total_events,
            total_purchases = dim_users.total_purchases + EXCLUDED.total_purchases,
            total_revenue = dim_users.total_revenue + EXCLUDED.total_revenue,
            first_purchase_timestamp = LEAST(dim_users.first_purchase_timestamp, EXCLUDED.first_purchase_timestamp),
            has_used_installments = dim_users.has_used_installments OR EXCLUDED.has_used_installments,
            total_view_items = dim_users.total_view_items + EXCLUDED.total_view_items,
            total_checkouts = dim_users.total_checkouts + EXCLUDED.total_checkouts,
            days_since_acquisition = today() - DATE(dim_users.first_seen_timestamp),
            lifetime_value = dim_users.total_revenue + EXCLUDED.total_revenue,
            updated_at = now()
    """
    )
    logger.info("Successfully upserted into dim_users.")

    logger.info("Upserting into dim_products...")
    con.execute(
        """
        WITH product_aggregates AS (
            -- Aggregate metrics per product
            SELECT
                product_id,
                MAX(product_name) AS product_name,
                COUNT(CASE WHEN event_name = 'view_item_list' THEN 1 END) AS total_impressions,
                COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) AS total_views,
                COUNT(CASE WHEN event_name = 'begin_checkout' THEN 1 END) AS total_checkouts,
                COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS total_purchases,
                SUM(CASE WHEN event_name = 'purchase' THEN item_revenue ELSE 0 END) AS total_revenue,
                COUNT(CASE WHEN event_name = 'purchase' AND installments > 0 THEN 1 END) AS total_purchases_with_installments,
                MIN(event_timestamp) AS first_seen_timestamp,
                MAX(event_timestamp) AS last_seen_timestamp
            FROM net_new
            WHERE product_id IS NOT NULL
            GROUP BY product_id
        ),
        latest_prices AS (
            -- Get the most recent price per product using window function (scalable for large volumes)
            SELECT DISTINCT
                product_id,
                FIRST_VALUE(price) OVER (
                    PARTITION BY product_id
                    ORDER BY event_timestamp DESC,
                             CASE WHEN price IS NOT NULL THEN 0 ELSE 1 END  -- Prefer non-null prices
                ) AS latest_price
            FROM net_new
            WHERE product_id IS NOT NULL
              AND event_name != 'view_item_list'  -- Exclude impressions
              AND price IS NOT NULL
        )
        INSERT INTO dim_products (
            product_id,
            product_name,
            total_impressions,
            total_views,
            total_checkouts,
            total_purchases,
            total_revenue,
            total_purchases_with_installments,
            latest_price,
            first_seen_timestamp,
            last_seen_timestamp,
            updated_at
        )
        SELECT
            pa.product_id,
            pa.product_name,
            pa.total_impressions,
            pa.total_views,
            pa.total_checkouts,
            pa.total_purchases,
            pa.total_revenue,
            pa.total_purchases_with_installments,
            lp.latest_price,
            pa.first_seen_timestamp,
            pa.last_seen_timestamp,
            now() AS updated_at
        FROM product_aggregates pa
        LEFT JOIN latest_prices lp ON pa.product_id = lp.product_id

        ON CONFLICT (product_id) DO UPDATE SET
            -- Update name if it changed (SCD Type 1 - overwrite)
            product_name = COALESCE(EXCLUDED.product_name, dim_products.product_name),

            -- Accumulate metrics from new batch
            total_impressions = dim_products.total_impressions + EXCLUDED.total_impressions,
            total_views = dim_products.total_views + EXCLUDED.total_views,
            total_checkouts = dim_products.total_checkouts + EXCLUDED.total_checkouts,
            total_purchases = dim_products.total_purchases + EXCLUDED.total_purchases,
            total_revenue = dim_products.total_revenue + EXCLUDED.total_revenue,
            total_purchases_with_installments = dim_products.total_purchases_with_installments + EXCLUDED.total_purchases_with_installments,

            -- Update latest_price only if new batch has more recent data
            latest_price = CASE
                WHEN EXCLUDED.last_seen_timestamp > dim_products.last_seen_timestamp
                     AND EXCLUDED.latest_price IS NOT NULL
                THEN EXCLUDED.latest_price
                WHEN EXCLUDED.latest_price IS NOT NULL
                     AND dim_products.latest_price IS NULL
                THEN EXCLUDED.latest_price
                ELSE dim_products.latest_price
            END,

            -- Keep earliest first_seen, latest last_seen
            first_seen_timestamp = LEAST(dim_products.first_seen_timestamp, EXCLUDED.first_seen_timestamp),
            last_seen_timestamp = GREATEST(dim_products.last_seen_timestamp, EXCLUDED.last_seen_timestamp),

            updated_at = now();
    """
    )
    logger.info("Successfully upserted into dim_products.")

    # Item lists with attribution logic
    logger.info("Upserting into dim_item_lists...")
    con.execute(
        """

        -- Seed lookup rows for any list IDs seen in this batch (no metrics yet)
            INSERT INTO dim_item_lists (
            item_list_id,
            item_list_name,
            first_seen_timestamp,
            last_seen_timestamp,
            updated_at
            )
            SELECT
            item_list_id,
            MAX(item_list_name),
            MIN(event_timestamp),
            MAX(event_timestamp),
            now()
            FROM net_new
            WHERE item_list_id IS NOT NULL
            GROUP BY item_list_id
            ON CONFLICT (item_list_id) DO NOTHING;

        -- Now calculate metrics for each list
        WITH batch_list_metrics AS (
            -- Impressions: Direct count of view_item_list events
            SELECT
                item_list_id,
                MAX(item_list_name) AS item_list_name,
                COUNT(*) AS total_impressions,
                COUNT(DISTINCT user_id) AS unique_users_exposed,
                MIN(event_timestamp) AS first_seen,
                MAX(event_timestamp) AS last_seen
            FROM net_new
            WHERE item_list_id IS NOT NULL
              AND event_name = 'view_item_list'
            GROUP BY item_list_id
        ),

        attributed_clicks AS (
            -- Attribute clicks/checkouts/purchases to the "current" list view within 30min
            -- Build indexed pool of candidate impressions (current batch + recent history)
            -- Windowed Union scales upto 100k rows per batch.

            WITH all_impressions AS (
                -- Current batch impressions (small, ~5-10K rows)
                SELECT
                    item_list_id,
                    user_id,
                    event_timestamp
                FROM net_new
                WHERE item_list_id IS NOT NULL
                AND event_name = 'view_item_list'

                UNION ALL

                -- Recent impressions from history (index scan on user_id + timestamp)
                -- Only fetch impressions within possible attribution window
                SELECT
                    item_list_id,
                    user_id,
                    event_timestamp
                FROM fct_events
                WHERE item_list_id IS NOT NULL
                AND event_name = 'view_item_list'
                AND event_timestamp >= (
                    SELECT MIN(event_timestamp) - INTERVAL '30 minutes'
                    FROM net_new
                )
            )
            SELECT
                e.event_id,
                e.user_id,
                e.event_name,
                e.item_revenue,
                (SELECT item_list_id
                 FROM all_impressions l
                 WHERE l.user_id = e.user_id
                   AND l.event_timestamp < e.event_timestamp
                   AND e.event_timestamp - l.event_timestamp <= INTERVAL '30 minutes'
                 ORDER BY l.event_timestamp DESC
                 LIMIT 1
                ) AS attributed_list_id
            FROM net_new e
            WHERE e.event_name IN ('view_item', 'begin_checkout', 'purchase')
              AND e.user_id IS NOT NULL
        ),

        attributed_metrics AS (
            SELECT
                attributed_list_id AS item_list_id,
                COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) AS total_clicks,
                COUNT(CASE WHEN event_name = 'begin_checkout' THEN 1 END) AS total_checkouts,
                COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS total_purchases,
                SUM(CASE WHEN event_name = 'purchase' THEN item_revenue ELSE 0 END) AS total_revenue
            FROM attributed_clicks
            WHERE attributed_list_id IS NOT NULL
            GROUP BY attributed_list_id
        ),

        combined_metrics AS (
            SELECT
                COALESCE(blm.item_list_id, am.item_list_id) AS item_list_id,
                blm.item_list_name,
                COALESCE(blm.total_impressions, 0) AS total_impressions,
                COALESCE(am.total_clicks, 0) AS total_attributed_clicks,
                COALESCE(am.total_checkouts, 0) AS total_checkouts,
                COALESCE(am.total_purchases, 0) AS total_purchases,
                COALESCE(am.total_revenue, 0) AS total_revenue,
                COALESCE(blm.unique_users_exposed, 0) AS unique_users_exposed,
                blm.first_seen,
                blm.last_seen
            FROM batch_list_metrics blm
            FULL OUTER JOIN attributed_metrics am ON blm.item_list_id = am.item_list_id
        )

        INSERT INTO dim_item_lists (
            item_list_id,
            item_list_name,
            total_impressions,
            total_attributed_clicks,
            total_checkouts,
            total_purchases,
            total_revenue,
            unique_users_exposed,
            first_seen_timestamp,
            last_seen_timestamp,
            updated_at
        )
        SELECT
            item_list_id,
            item_list_name,
            total_impressions,
            total_attributed_clicks,
            total_checkouts,
            total_purchases,
            total_revenue,
            unique_users_exposed,
            first_seen,
            last_seen,
            now()
        FROM combined_metrics
        WHERE item_list_id IS NOT NULL

        ON CONFLICT (item_list_id) DO UPDATE SET
            item_list_name = COALESCE(EXCLUDED.item_list_name, dim_item_lists.item_list_name),
            total_impressions = dim_item_lists.total_impressions + EXCLUDED.total_impressions,
            total_attributed_clicks = dim_item_lists.total_attributed_clicks + EXCLUDED.total_attributed_clicks,
            total_checkouts = dim_item_lists.total_checkouts + EXCLUDED.total_checkouts,
            total_purchases = dim_item_lists.total_purchases + EXCLUDED.total_purchases,
            total_revenue = dim_item_lists.total_revenue + EXCLUDED.total_revenue,
            unique_users_exposed = dim_item_lists.unique_users_exposed + EXCLUDED.unique_users_exposed,
            first_seen_timestamp = LEAST(dim_item_lists.first_seen_timestamp, EXCLUDED.first_seen_timestamp),
            last_seen_timestamp = GREATEST(dim_item_lists.last_seen_timestamp, EXCLUDED.last_seen_timestamp),
            updated_at = now();
    """
    )
    logger.info("Successfully upserted into dim_item_lists.")

    # Campaigns: Enhanced with performance metrics and multi-campaign splitting
    logger.info("Processing dim_campaigns...")
    con.execute(
        """
        WITH exploded_campaigns AS (
            SELECT
                event_id,
                event_name,
                event_timestamp,
                user_id,
                item_revenue,
                installments,
                UNNEST(string_split(discount_campaigns, '|')) AS campaign_code
            FROM net_new
            WHERE discount_campaigns IS NOT NULL
        ),
        campaign_metrics AS (
            SELECT
                campaign_code AS campaign_id,
                campaign_code AS campaign_name,
                MIN(event_timestamp)::DATE AS start_date,
                MAX(event_timestamp)::DATE AS end_date,
                COUNT(DISTINCT user_id) AS total_users,
                COUNT(*) AS total_events,
                COUNT(CASE WHEN event_name = 'view_item_list' THEN 1 END) AS total_impressions,
                COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) AS total_views,
                COUNT(CASE WHEN event_name = 'begin_checkout' THEN 1 END) AS total_checkouts,
                COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS total_purchases,
                SUM(CASE WHEN event_name = 'purchase' THEN item_revenue ELSE 0 END) AS total_revenue,
                COUNT(CASE WHEN event_name = 'purchase' AND installments > 0 THEN 1 END) AS purchases_with_installments,
                MIN(event_timestamp) AS first_seen,
                MAX(event_timestamp) AS last_seen
            FROM exploded_campaigns
            GROUP BY campaign_code
        )
        INSERT INTO dim_campaigns (
            campaign_id, campaign_name,
            logged_start_date, logged_end_date, is_active,
            total_users, total_events, total_impressions, total_views, total_checkouts, total_purchases,
            total_revenue, avg_order_value, purchases_with_installments, installment_adoption_rate,
            first_seen_timestamp, last_seen_timestamp, updated_at
        )
        SELECT
            campaign_id, campaign_name,
            start_date, end_date, (end_date >= today()),
            total_users, total_events, total_impressions, total_views, total_checkouts, total_purchases,
            total_revenue,
            (total_revenue / NULLIF(total_purchases, 0)),
            purchases_with_installments,
            (purchases_with_installments::DECIMAL / NULLIF(total_purchases, 0)) * 100,
            first_seen, last_seen, now()
        FROM campaign_metrics
        ON CONFLICT (campaign_id) DO UPDATE SET
            campaign_name = COALESCE(EXCLUDED.campaign_name, dim_campaigns.campaign_name),
            logged_end_date = GREATEST(dim_campaigns.logged_end_date, EXCLUDED.logged_end_date),
            is_active = (GREATEST(dim_campaigns.logged_end_date, EXCLUDED.logged_end_date) >= today()),
            total_users = dim_campaigns.total_users + EXCLUDED.total_users,
            total_events = dim_campaigns.total_events + EXCLUDED.total_events,
            total_impressions = dim_campaigns.total_impressions + EXCLUDED.total_impressions,
            total_views = dim_campaigns.total_views + EXCLUDED.total_views,
            total_checkouts = dim_campaigns.total_checkouts + EXCLUDED.total_checkouts,
            total_purchases = dim_campaigns.total_purchases + EXCLUDED.total_purchases,
            total_revenue = dim_campaigns.total_revenue + EXCLUDED.total_revenue,
            purchases_with_installments = dim_campaigns.purchases_with_installments + EXCLUDED.purchases_with_installments,
            avg_order_value = (dim_campaigns.total_revenue + EXCLUDED.total_revenue) /
                            NULLIF(dim_campaigns.total_purchases + EXCLUDED.total_purchases, 0),
            installment_adoption_rate = ((dim_campaigns.purchases_with_installments + EXCLUDED.purchases_with_installments)::DECIMAL /
                                        NULLIF(dim_campaigns.total_purchases + EXCLUDED.total_purchases, 0)) * 100,
            first_seen_timestamp = LEAST(dim_campaigns.first_seen_timestamp, EXCLUDED.first_seen_timestamp),
            last_seen_timestamp = GREATEST(dim_campaigns.last_seen_timestamp, EXCLUDED.last_seen_timestamp),
            updated_at = now();
    """
    )
    logger.info("Successfully upserted into dim_campaigns.")

    # Experiments: Enhanced with conversion and revenue metrics
    logger.info("Processing dim_experiments...")
    con.execute(
        """
        WITH experiment_metrics AS (
            SELECT
                discount_experiment_variant AS experiment_id,
                discount_experiment_variant AS experiment_name,
                discount_experiment_variant AS variant_name,
                MIN(event_timestamp)::DATE AS start_date,
                MAX(event_timestamp)::DATE AS end_date,
                COUNT(DISTINCT user_id) AS total_users,
                COUNT(*) AS total_events,
                COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) AS total_views,
                COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS total_purchases,
                SUM(CASE WHEN event_name = 'purchase' THEN item_revenue ELSE 0 END) AS total_revenue,
                MIN(event_timestamp) AS first_seen,
                MAX(event_timestamp) AS last_seen
            FROM net_new
            WHERE discount_experiment_variant IS NOT NULL
            GROUP BY discount_experiment_variant
        )
        INSERT INTO dim_experiments (
            experiment_id, experiment_name, variant_name,
            logged_start_date, logged_end_date, is_active,
            total_users, total_events, total_views, total_purchases, conversion_rate,
            total_revenue, avg_revenue_per_user, sample_size_achieved,
            first_seen_timestamp, last_seen_timestamp, updated_at
        )
        SELECT
            experiment_id, experiment_name, variant_name,
            start_date, end_date, (end_date >= today()),
            total_users, total_events, total_views, total_purchases,
            (total_purchases::DECIMAL / NULLIF(total_views, 0)) * 100,
            total_revenue,
            (total_revenue / NULLIF(total_users, 0)),
            total_users,
            first_seen, last_seen, now()
        FROM experiment_metrics
        ON CONFLICT (experiment_id) DO UPDATE SET
            experiment_name = COALESCE(EXCLUDED.experiment_name, dim_experiments.experiment_name),
            logged_end_date = GREATEST(dim_experiments.logged_end_date, EXCLUDED.logged_end_date),
            is_active = (GREATEST(dim_experiments.logged_end_date, EXCLUDED.logged_end_date) >= today()),
            total_users = dim_experiments.total_users + EXCLUDED.total_users,
            total_events = dim_experiments.total_events + EXCLUDED.total_events,
            total_views = dim_experiments.total_views + EXCLUDED.total_views,
            total_purchases = dim_experiments.total_purchases + EXCLUDED.total_purchases,
            total_revenue = dim_experiments.total_revenue + EXCLUDED.total_revenue,
            conversion_rate = ((dim_experiments.total_purchases + EXCLUDED.total_purchases)::DECIMAL /
                             NULLIF(dim_experiments.total_views + EXCLUDED.total_views, 0)) * 100,
            avg_revenue_per_user = (dim_experiments.total_revenue + EXCLUDED.total_revenue) /
                                  NULLIF(dim_experiments.total_users + EXCLUDED.total_users, 0),
            sample_size_achieved = dim_experiments.total_users + EXCLUDED.total_users,
            first_seen_timestamp = LEAST(dim_experiments.first_seen_timestamp, EXCLUDED.first_seen_timestamp),
            last_seen_timestamp = GREATEST(dim_experiments.last_seen_timestamp, EXCLUDED.last_seen_timestamp),
            updated_at = now();
    """
    )
    logger.info("Successfully upserted into dim_experiments.")

    # Fact insert - load all immutable measures
    # See plan/fct_events_design_analysis.md for field rationale
    logger.info("Loading fct_events...")
    con.execute(
        """
        INSERT INTO fct_events (
            -- Identifiers
            event_id,
            event_timestamp,
            event_name,

            -- Foreign keys
            user_id,
            product_id,
            item_list_id,
            platform_id,
            date_id,

            -- Degenerate dimensions (store codes as-is)
            campaign_codes,
            experiment_id,

            -- Pricing measures
            price_in_usd,
            price,
            quantity,
            item_revenue_in_usd,
            item_revenue,

            -- Discount measures
            discount_value,
            total_price,

            -- Payment measures
            installments,
            installment_price,

            -- Product state
            is_in_stock,

            -- Audit
            ingestion_timestamp
        )
        SELECT
            -- Identifiers
            stg.event_id,
            stg.event_timestamp,
            stg.event_name,

            -- Foreign keys (resolve platform to ID)
            stg.user_id,
            stg.product_id,
            stg.item_list_id,
            p.platform_id,
            DATE(stg.event_timestamp) AS date_id,

            -- Degenerate dimensions (campaigns are pipe-joined strings)
            stg.discount_campaigns AS campaign_codes,
            stg.discount_experiment_variant AS experiment_id,

            -- Pricing measures (preserve full precision)
            stg.price_in_usd,
            stg.price,
            stg.quantity,
            stg.item_revenue_in_usd,
            stg.item_revenue,

            -- Discount measures
            stg.discount_value,
            stg.total_price,

            -- Payment measures (critical for fintech metrics)
            stg.installments,
            stg.installment_price,

            -- Product state (point-in-time snapshot)
            stg.is_in_stock,

            -- Audit
            stg.ingestion_timestamp

        FROM net_new stg
        LEFT JOIN dim_platforms p ON UPPER(stg.platform) = p.platform_name
        ON CONFLICT (event_id) DO NOTHING;
    """
    )
    logger.info("Successfully loaded into fct_events.")

    # Mark staged rows as processed
    logger.info("Marking staged events as processed...")
    con.execute(
        """
        UPDATE stg_events
        SET processed_at = NOW()
        WHERE processed_at IS NULL
        AND event_id IN (SELECT event_id FROM current_batch);
    """
    )
    logger.info("Successfully marked staged events as processed.")


# def main():
#     run_ts = os.getenv('RUN_TS') or datetime.now().strftime('%Y%m%d_%H%M%S')
#     logger, _fmt = configure_logging(f"logs/silver_transforms_{run_ts}.log", logger_name='silver')
#     logger.info("--- Starting Silver Transforms ---")

#     con = duckdb.connect(database=DB_FILE, read_only=False)
#     try:
#         con.begin()
#         run_silver_transforms(con, logger)
#         con.commit()
#         logger.info("Silver transforms committed.")
#     except Exception as e:
#         con.rollback()
#         logger.error(f"Silver transforms failed and were rolled back: {e}", exc_info=True)
#         raise
#     finally:
#         if con:
#             con.close()
#         logger.info("--- Silver Transforms Finished ---")


# if __name__ == "__main__":
#     main()
