import logging

import duckdb

from logging_utils import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

DB_PATH = "nelo_growth_analytics.db"


def run_gold_transforms():
    """
    Main function to run all gold layer transformations.
    Connects to DuckDB and executes the transform functions.
    """
    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logger.info("Successfully connected to DuckDB.")

        # Gold layer transformations
        populate_gold_platform_daily_performance(con)
        populate_gold_campaign_daily_performance(con)
        populate_gold_product_daily_performance(con)

        logger.info("Gold layer transformations completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during gold transformations: {e}")
    finally:
        if "con" in locals() and con:
            con.close()
            logger.info("DuckDB connection closed.")


def populate_gold_platform_daily_performance(con):
    """
    Populates the gold_platform_daily_performance table by aggregating
    data from fct_events. This provides a daily snapshot of platform metrics.
    """
    logger.info("Populating gold_platform_daily_performance...")
    # Logic from plan/dim_platforms_strategic_analysis.md
    con.execute(
        """
        INSERT INTO gold_platform_daily_performance
        SELECT
            DATE(event_timestamp) as date,
            platform_id,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(CASE WHEN event_name = 'view_item_list' THEN 1 END) as total_impressions,
            COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) as total_views,
            COUNT(CASE WHEN event_name = 'begin_checkout' THEN 1 END) as total_checkouts,
            COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) as total_purchases,
            SUM(CASE WHEN event_name = 'purchase' THEN item_revenue_in_usd ELSE 0 END) as total_revenue,
            AVG(CASE WHEN event_name = 'purchase' THEN item_revenue_in_usd END) as avg_order_value,
            COUNT(CASE WHEN event_name = 'purchase' AND installments > 0 THEN 1 END) as purchases_with_installments,
            (COUNT(CASE WHEN event_name = 'purchase' AND installments > 0 THEN 1 END)::DECIMAL /
             NULLIF(COUNT(CASE WHEN event_name = 'purchase' THEN 1 END), 0)) * 100 as installment_adoption_rate
        FROM fct_events
        GROUP BY DATE(event_timestamp), platform_id
        ON CONFLICT (date, platform_id) DO UPDATE SET
            total_events = EXCLUDED.total_events,
            unique_users = EXCLUDED.unique_users,
            total_impressions = EXCLUDED.total_impressions,
            total_views = EXCLUDED.total_views,
            total_checkouts = EXCLUDED.total_checkouts,
            total_purchases = EXCLUDED.total_purchases,
            total_revenue = EXCLUDED.total_revenue,
            avg_order_value = EXCLUDED.avg_order_value,
            purchases_with_installments = EXCLUDED.purchases_with_installments,
            installment_adoption_rate = EXCLUDED.installment_adoption_rate;
    """
    )
    logger.info("Finished populating gold_platform_daily_performance.")


def populate_gold_campaign_daily_performance(con):
    """
    Populates gold_campaign_daily_performance table.
    This involves unnesting the pipe-separated campaign codes from fct_events
    to correctly attribute events to each campaign.
    """
    logger.info("Populating gold_campaign_daily_performance...")
    # Logic from plan/dim_campaigns_experiments_analysis.md
    con.execute(
        """
        INSERT INTO gold_campaign_daily_performance
        WITH exploded AS (
            SELECT
                DATE(event_timestamp) as date,
                UNNEST(string_split(campaign_codes, '|')) AS campaign_id,
                platform_id,
                event_name,
                user_id,
                item_revenue_in_usd,
                installments
            FROM fct_events
            WHERE campaign_codes IS NOT NULL
        )
        SELECT
            date,
            campaign_id,
            platform_id,
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS total_purchases,
            SUM(CASE WHEN event_name = 'purchase' THEN item_revenue_in_usd ELSE 0 END) AS total_revenue,
            (COUNT(CASE WHEN event_name = 'purchase' THEN 1 END)::DECIMAL /
             NULLIF(COUNT(CASE WHEN event_name = 'view_item' THEN 1 END), 0)) * 100 AS conversion_rate,
            (COUNT(CASE WHEN event_name = 'purchase' AND installments > 0 THEN 1 END)::DECIMAL /
             NULLIF(COUNT(CASE WHEN event_name = 'purchase' THEN 1 END), 0)) * 100 AS installment_adoption_rate
        FROM exploded
        GROUP BY date, campaign_id, platform_id
        ON CONFLICT (date, campaign_id, platform_id) DO UPDATE SET
            unique_users = EXCLUDED.unique_users,
            total_purchases = EXCLUDED.total_purchases,
            total_revenue = EXCLUDED.total_revenue,
            conversion_rate = EXCLUDED.conversion_rate,
            installment_adoption_rate = EXCLUDED.installment_adoption_rate;
    """
    )
    logger.info("Finished populating gold_campaign_daily_performance.")


def populate_gold_product_daily_performance(con):
    """
    Populates gold_product_daily_performance table with daily product metrics.
    """
    logger.info("Populating gold_product_daily_performance...")
    con.execute(
        """
        INSERT INTO gold_product_daily_performance (
            date,
            product_id,
            total_impressions,
            total_views,
            total_checkouts,
            total_purchases,
            total_revenue,
            conversion_rate,
            avg_order_value
        )
        SELECT
            DATE(event_timestamp) AS date,
            product_id,
            COUNT(CASE WHEN event_name = 'view_item_list' THEN 1 END) AS total_impressions,
            COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) AS total_views,
            COUNT(CASE WHEN event_name = 'begin_checkout' THEN 1 END) AS total_checkouts,
            COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS total_purchases,
            SUM(CASE WHEN event_name = 'purchase' THEN item_revenue_in_usd ELSE 0 END) AS total_revenue,
            (COUNT(CASE WHEN event_name = 'purchase' THEN 1 END)::DECIMAL / NULLIF(COUNT(CASE WHEN event_name = 'view_item' THEN 1 END), 0)) * 100 AS conversion_rate,
            (SUM(CASE WHEN event_name = 'purchase' THEN item_revenue_in_usd ELSE 0 END) / NULLIF(COUNT(CASE WHEN event_name = 'purchase' THEN 1 END), 0)) AS avg_order_value
        FROM fct_events
        WHERE product_id IS NOT NULL
        GROUP BY DATE(event_timestamp), product_id
        ON CONFLICT (date, product_id) DO UPDATE SET
            total_impressions = EXCLUDED.total_impressions,
            total_views = EXCLUDED.total_views,
            total_checkouts = EXCLUDED.total_checkouts,
            total_purchases = EXCLUDED.total_purchases,
            total_revenue = EXCLUDED.total_revenue,
            conversion_rate = EXCLUDED.conversion_rate,
            avg_order_value = EXCLUDED.avg_order_value;
    """
    )
    logger.info("Finished populating gold_product_daily_performance.")


if __name__ == "__main__":
    run_gold_transforms()
