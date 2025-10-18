import logging

import duckdb

# --- Configuration ---
DB_FILE = "nelo_growth_analytics.db"
LOG_FILE = "etl.log"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, mode="a"), logging.StreamHandler()],
)


def setup_database():
    """
    Connects to the DuckDB database and creates the necessary tables
    if they don't exist, as per the schema in plan.md.
    """
    con = duckdb.connect(DB_FILE)
    logging.info(f"Successfully connected to DuckDB database: {DB_FILE}")

    # Unified Staging Table - APPEND-ONLY (allows duplicates)
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS stg_events (
    -- Event metadata (no primary key - allows duplicates from SQS at-least-once delivery)
    event_id VARCHAR,
    event_name VARCHAR,
    event_timestamp TIMESTAMP,
    replay_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    user_id VARCHAR,
    platform VARCHAR,

    -- Product info
    item_list_id VARCHAR,
    item_list_name VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,

    -- Pricing
    price_in_usd DECIMAL(12, 6),
    price DECIMAL(12, 2),
    quantity INTEGER,
    item_revenue_in_usd DECIMAL(12, 6),
    item_revenue DECIMAL(12, 2),

    -- Discounts & campaigns
    discount_value DECIMAL(10, 2),
    total_price DECIMAL(10, 2),
    discount_campaigns VARCHAR,
    discount_experiment_variant VARCHAR,

    -- Other item params
    installments INTEGER,
    installment_price DECIMAL(10, 2),
    is_in_stock BOOLEAN,

    -- Audit
    raw_event JSON,
    processed_at TIMESTAMP
    );
    """
    )

    # Non-unique index for query performance (no uniqueness constraint)
    con.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_stg_dedup_key
    ON stg_events(event_id, ingestion_timestamp);
    """
    )

    # Index for filtering unprocessed rows (DuckDB doesn't support partial indexes)
    con.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_stg_processed
    ON stg_events(processed_at);
    """
    )

    logging.info("Table 'stg_events' is set up (append-only, duplicates allowed).")

    # Dimension Tables

    # Date Dimension Creation
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_dates (
        date_key DATE PRIMARY KEY,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        day_of_week INTEGER,  -- 0=Sunday, 1=Monday, etc.
        is_weekend BOOLEAN,
        -- Optional nice-to-haves:
        month_name VARCHAR,    -- 'January', 'February', etc.
        quarter INTEGER        -- 1, 2, 3, 4
    );
    """
    )

    # Users Dimension Creation
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_users (
        user_id VARCHAR PRIMARY KEY,
        first_seen_timestamp TIMESTAMP,        -- Immutable after first set
        acquisition_week DATE,                 -- For cohort analysis
        acquisition_month DATE,                -- For monthly cohorts

        -- Aggregated metrics (updated each batch)
        total_events INTEGER DEFAULT 0,
        total_purchases INTEGER DEFAULT 0,
        total_revenue DECIMAL(12,2) DEFAULT 0,
        first_purchase_timestamp TIMESTAMP,
        has_used_installments BOOLEAN DEFAULT false,

        -- Lifecycle tracking
        last_seen_timestamp TIMESTAMP,         -- Keep for convenience
        days_since_acquisition INTEGER,        -- Derived: days between first_seen and now
        lifetime_value DECIMAL(12,2),          -- Sum of all purchase revenue

        -- Engagement scoring
        total_view_items INTEGER DEFAULT 0,
        total_checkouts INTEGER DEFAULT 0,

        -- Audit
        updated_at TIMESTAMP
    );
    """
    )

    con.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_products (
        product_id VARCHAR PRIMARY KEY,
        product_name VARCHAR,
        msrp_in_usd DECIMAL(12, 2),

        -- Aggregated metrics (updated each batch)
        total_impressions INTEGER DEFAULT 0,      -- Count of view_item_list events
        total_views INTEGER DEFAULT 0,             -- Count of view_item events
        total_checkouts INTEGER DEFAULT 0,         -- Count of begin_checkout events
        total_purchases INTEGER DEFAULT 0,         -- Count of purchase events
        total_revenue DECIMAL(12,2) DEFAULT 0,     -- Sum of item_revenue_in_usd
        total_purchases_with_installments INTEGER DEFAULT 0,
        latest_price DECIMAL(12, 2) DEFAULT 0,

        -- Audit
        first_seen_timestamp TIMESTAMP,
        last_seen_timestamp TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    )
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_item_lists (
        item_list_id VARCHAR PRIMARY KEY,
        item_list_name VARCHAR,

        -- List performance metrics (updated each batch)
        total_impressions INTEGER DEFAULT 0,
        total_attributed_clicks INTEGER DEFAULT 0,
        total_checkouts INTEGER DEFAULT 0,
        total_purchases INTEGER DEFAULT 0,
        total_revenue DECIMAL(12,2) DEFAULT 0,
        unique_users_exposed INTEGER DEFAULT 0,

        -- Audit
        first_seen_timestamp TIMESTAMP,
        last_seen_timestamp TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    )

    # dim_platforms: Minimal lookup table (no aggregates)
    # Platform metrics belong in Gold layer (gold_platform_daily_performance)
    # See plan/dim_platforms_strategic_analysis.md for rationale
    con.execute(
        "CREATE TABLE IF NOT EXISTS dim_platforms (platform_id INTEGER PRIMARY KEY, platform_name VARCHAR UNIQUE);"
    )

    # dim_campaigns: Enhanced with performance metrics
    # See plan/dim_campaigns_experiments_analysis.md
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_campaigns (
        campaign_id VARCHAR PRIMARY KEY,
        campaign_name VARCHAR,
        planned_start_date DATE,
        planned_end_date DATE,
        is_active BOOLEAN,

        logged_start_date DATE,
        logged_end_date DATE,
        total_users INTEGER DEFAULT 0,
        total_events INTEGER DEFAULT 0,
        total_impressions INTEGER DEFAULT 0,
        total_views INTEGER DEFAULT 0,
        total_checkouts INTEGER DEFAULT 0,
        total_purchases INTEGER DEFAULT 0,

        total_revenue DECIMAL(12,2) DEFAULT 0,
        avg_order_value DECIMAL(12,2),

        purchases_with_installments INTEGER DEFAULT 0,
        installment_adoption_rate DECIMAL(5,2),

        first_seen_timestamp TIMESTAMP,
        last_seen_timestamp TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    )

    # dim_experiments: Enhanced with conversion and revenue metrics
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_experiments (
        experiment_id VARCHAR PRIMARY KEY,
        experiment_name VARCHAR,
        variant_name VARCHAR,
        planned_start_date DATE,
        planned_end_date DATE,

        logged_start_date DATE,
        logged_end_date DATE,
        is_active BOOLEAN,

        total_users INTEGER DEFAULT 0,
        total_events INTEGER DEFAULT 0,

        total_views INTEGER DEFAULT 0,
        total_purchases INTEGER DEFAULT 0,
        conversion_rate DECIMAL(5,2),

        total_revenue DECIMAL(12,2) DEFAULT 0,
        avg_revenue_per_user DECIMAL(12,2),

        sample_size_target INTEGER,
        sample_size_achieved INTEGER,

        first_seen_timestamp TIMESTAMP,
        last_seen_timestamp TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    )

    con.execute(
        "INSERT INTO dim_platforms (platform_id, platform_name) VALUES (1, 'IOS'), (2, 'ANDROID') ON CONFLICT DO NOTHING;"
    )
    logging.info("Dimension tables are set up.")

    # Fact Table - Enhanced with all immutable measures
    # See plan/fct_events_design_analysis.md for design rationale
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS fct_events (
        -- Event identifiers
        event_id VARCHAR PRIMARY KEY,
        event_timestamp TIMESTAMP,
        event_name VARCHAR,

        -- Foreign keys to dimensions
        user_id VARCHAR REFERENCES dim_users(user_id),
        product_id VARCHAR REFERENCES dim_products(product_id),
        item_list_id VARCHAR REFERENCES dim_item_lists(item_list_id),
        platform_id INTEGER REFERENCES dim_platforms(platform_id),
        date_id DATE,

        -- Degenerate dimensions (codes stored as-is, no FK)
        campaign_codes VARCHAR,
        experiment_id VARCHAR,

        -- Pricing measures (prALTER TABLE dim_products RENAME latest_price_in_usd to latest_price;eserve precision from staging)
        price_in_usd DECIMAL(12, 6),
        price DECIMAL(12, 2),
        quantity INTEGER,
        item_revenue_in_usd DECIMAL(12, 6),
        item_revenue DECIMAL(12, 2),

        -- Discount measures
        discount_value DECIMAL(10, 2),
        total_price DECIMAL(10, 2),

        -- Payment measures (critical for fintech analysis)
        installments INTEGER,
        installment_price DECIMAL(10, 2),

        -- Product state (point-in-time snapshot)
        is_in_stock BOOLEAN,

        -- Audit metadata
        ingestion_timestamp TIMESTAMP
    );
    """
    )

    # Performance indexes
    con.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_fct_user_ts ON fct_events (user_id, event_timestamp);
    """
    )
    con.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_fct_date ON fct_events (date_id);
    """
    )
    # Indexes consume ~30% more disk space. to be added based on query performance.
    # con.execute("""
    # CREATE INDEX IF NOT EXISTS idx_fct_product ON fct_events (product_id, event_timestamp);
    # """)
    # con.execute("""
    # CREATE INDEX IF NOT EXISTS idx_fct_event_name ON fct_events (event_name);
    # """)
    logging.info("Fact table 'fct_events' is set up.")

    # Gold Tables - Business-Ready Aggregates
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS gold_platform_daily_performance (
        date DATE,
        platform_id INTEGER REFERENCES dim_platforms(platform_id),

        -- Engagement
        total_events INTEGER,
        unique_users INTEGER,

        -- Funnel
        total_impressions INTEGER,
        total_views INTEGER,
        total_checkouts INTEGER,
        total_purchases INTEGER,

        -- Revenue
        total_revenue DECIMAL(12,2),
        avg_order_value DECIMAL(12,2),

        -- Fintech
        purchases_with_installments INTEGER,
        installment_adoption_rate DECIMAL(5,2),

        PRIMARY KEY (date, platform_id)
    );
    """
    )

    con.execute(
        """
    CREATE TABLE IF NOT EXISTS gold_campaign_daily_performance (
        date DATE,
        campaign_id VARCHAR REFERENCES dim_campaigns(campaign_id),
        platform_id INTEGER REFERENCES dim_platforms(platform_id),

        unique_users INTEGER,
        total_purchases INTEGER,
        total_revenue DECIMAL(12,2),
        conversion_rate DECIMAL(5,2),
        installment_adoption_rate DECIMAL(5,2),

        PRIMARY KEY (date, campaign_id, platform_id)
    );
    """
    )

    con.execute(
        """
    CREATE TABLE IF NOT EXISTS gold_product_daily_performance (
        date DATE,
        product_id VARCHAR REFERENCES dim_products(product_id),

        total_impressions INTEGER,
        total_views INTEGER,
        total_checkouts INTEGER,
        total_purchases INTEGER,
        total_revenue DECIMAL(12,2),
        conversion_rate DECIMAL(5,2),
        avg_order_value DECIMAL(12,2),

        PRIMARY KEY (date, product_id)
    );
    """
    )

    logging.info("Gold tables are set up.")

    # Date Dimension
    con.execute(
        """
    -- Generate date dimension for 10 years
        INSERT INTO dim_dates
        SELECT
            date_key,
            YEAR(date_key) as year,
            MONTH(date_key) as month,
            DAY(date_key) as day,
            DAYOFWEEK(date_key) as day_of_week,
            CASE WHEN DAYOFWEEK(date_key) IN (0, 6) THEN true ELSE false END as is_weekend,
            MONTHNAME(date_key) as month_name,
            QUARTER(date_key) as quarter
        FROM (
            SELECT DATE '2020-01-01' + INTERVAL (n) DAY as date_key
            FROM generate_series(0, 3652) as t(n)  -- ~10 years
        )
        ON CONFLICT DO NOTHING;
    """
    )
    con.close()
    logging.info("Database setup complete. Connection closed.")


if __name__ == "__main__":
    logging.info("--- Starting Database Setup ---")
    setup_database()
    logging.info("--- Database Setup Finished ---")
