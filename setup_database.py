import logging

import duckdb

# --- Configuration ---
DB_FILE = "nelo_analytics_rewrite.db"
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
        CREATE SEQUENCE IF NOT EXISTS user_sk_seq START 1;

        CREATE TABLE IF NOT EXISTS dim_users (
        user_sk BIGINT PRIMARY KEY DEFAULT NEXTVAL('user_sk_seq'),
        user_id VARCHAR NOT NULL,                     -- NK
        
        source VARCHAR,
        region VARCHAR,
        customer_segment VARCHAR,
        risk_segment VARCHAR,
        credit_available DECIMAL(12,2),
        
        -- Attributes (no aggregates)
        first_purchase_date DATE,
        has_used_installments BOOLEAN,
        -- SCD window
        effective_start_date DATE NOT NULL,
        effective_end_date DATE,                      -- NULL = open
        is_active BOOLEAN DEFAULT TRUE,
        UNIQUE (user_id, effective_start_date)
        );
    """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_users_nk ON dim_users(user_id);
        CREATE INDEX IF NOT EXISTS idx_users_range ON dim_users(user_id, effective_start_date, effective_end_date);
        """
    )

    con.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS product_sk_seq START 1;

        CREATE TABLE IF NOT EXISTS dim_products (
            product_sk BIGINT PRIMARY KEY DEFAULT NEXTVAL('product_sk_seq'),
            product_id VARCHAR NOT NULL,                  -- NK
            product_name VARCHAR,
            product_category VARCHAR,               -- from product catalog upstream
            price DECIMAL(12,2),

            -- SCD window
            effective_start_date DATE NOT NULL,
            effective_end_date DATE,
            is_active BOOLEAN DEFAULT TRUE,
            UNIQUE (product_id, effective_start_date)
        );
    """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_products_nk ON dim_products(product_id);
        CREATE INDEX IF NOT EXISTS idx_products_range ON dim_products(product_id, effective_start_date, effective_end_date);
        """
    )    


    con.execute(
    """
    CREATE SEQUENCE IF NOT EXISTS item_list_sk_seq START 1;

    CREATE TABLE IF NOT EXISTS dim_item_lists (
        item_list_sk BIGINT PRIMARY KEY DEFAULT NEXTVAL('item_list_sk_seq'),
        item_list_id VARCHAR NOT NULL,                 -- NK
        item_list_name VARCHAR,

        -- from item list catalog upstream
        list_type VARCHAR,
        targeting_customer_segment VARCHAR,
        targeting_region VARCHAR,
        associated_experiment_variant VARCHAR,
        planned_start_date DATE,
        planned_end_date DATE,
        
        -- SCD window
        effective_start_date DATE NOT NULL,
        effective_end_date DATE,
        is_active BOOLEAN DEFAULT TRUE,
        UNIQUE (item_list_id, effective_start_date)
    );
    """
    )


    # dim_platforms: Minimal lookup table (no aggregates)
    con.execute(
        "CREATE TABLE IF NOT EXISTS dim_platforms (platform_id INTEGER PRIMARY KEY, platform_name VARCHAR UNIQUE);"
    )
    con.execute(
        "INSERT INTO dim_platforms (platform_id, platform_name) VALUES (1, 'IOS'), (2, 'ANDROID') ON CONFLICT DO NOTHING;"
    )

    # dim_campaigns: Enhanced with performance metrics
    con.execute(
    """
    CREATE SEQUENCE IF NOT EXISTS campaign_sk_seq START 1;

    CREATE TABLE IF NOT EXISTS dim_campaigns (
        campaign_sk BIGINT PRIMARY KEY DEFAULT NEXTVAL('campaign_sk_seq'),
        campaign_id VARCHAR NOT NULL,                 -- NK
        campaign_name VARCHAR,
        
        -- from campaign catalog upstream
        targeting_source VARCHAR,
        targeting_segment VARCHAR,
        targeting_risk_segment VARCHAR,
        planned_start_date DATE,
        planned_end_date DATE,

        -- SCD window
        effective_start_date DATE NOT NULL,           
        effective_end_date DATE,                      
        is_active BOOLEAN DEFAULT TRUE,
        UNIQUE (campaign_id, effective_start_date)
    );
    """
    )


    # dim_experiments: Enhanced with conversion and revenue metrics
    con.execute(
    """
    CREATE SEQUENCE IF NOT EXISTS experiment_sk_seq START 1;
        
    CREATE TABLE IF NOT EXISTS dim_experiments (
        experiment_sk BIGINT PRIMARY KEY DEFAULT NEXTVAL('experiment_sk_seq'),
        experiment_id VARCHAR NOT NULL,               -- NK
        experiment_name VARCHAR,
        
        -- from experiment catalog upstream
        variant_name VARCHAR,
        targeting_source VARCHAR,
        targeting_segment VARCHAR,
        targeting_risk_segment VARCHAR,
        planned_start_date DATE,
        planned_end_date DATE,
        
        -- SCD window
        effective_start_date DATE NOT NULL,           -- observed
        effective_end_date DATE,
        is_active BOOLEAN DEFAULT TRUE,
        UNIQUE (experiment_id, effective_start_date)
    );
    """
    )

    logging.info("Dimension tables are set up.")

    # Fact Table - Enhanced with all immutable measures
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS fct_events (
        -- Event identifiers
        event_id VARCHAR PRIMARY KEY,
        event_timestamp TIMESTAMP,
        event_name VARCHAR,

        -- Foreign keys to dimensions
        user_id VARCHAR, 
        product_id VARCHAR,
        item_list_id VARCHAR,
        platform_id INTEGER,
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


    # Silver persisted attribution (reusable in Gold)
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS silver_event_list_attribution (
        event_id VARCHAR PRIMARY KEY,
        user_id VARCHAR,
        event_name VARCHAR,
        event_timestamp TIMESTAMP,
        item_list_id VARCHAR,
        platform_id INTEGER,
        session_id VARCHAR,
        impression_ts_chosen TIMESTAMP,
        attribution_model VARCHAR,
        window_minutes INTEGER,
        attribution_version INTEGER,
        date_id DATE
    );
    """
    )


    # Persisted silver list impressions to support session-based, last-touch attribution
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS silver_list_impressions (
        user_id VARCHAR,
        platform_id INTEGER,
        session_id VARCHAR,
        session_start_ts TIMESTAMP,
        session_date DATE,
        item_list_id VARCHAR,
        impression_ts TIMESTAMP,
        date_id DATE,
        PRIMARY KEY (user_id, platform_id, session_start_ts, item_list_id, impression_ts)
    );
    """
    )

    logging.info("Creating silver sessions tables...")
    con.execute(
        """
        -- 1) Canonical sessions (persisted in Silver)
        CREATE TABLE IF NOT EXISTS silver_user_sessions (
            user_id VARCHAR,
            platform_id INTEGER,
            session_start_ts TIMESTAMP,
            session_end_ts TIMESTAMP,
            session_date DATE,
            events_in_session INTEGER,
            views INTEGER,
            checkouts INTEGER,
            purchases INTEGER,
            PRIMARY KEY (user_id, platform_id, session_start_ts)
        );

        -- 2) Session features (persisted in Silver) for targeting
        CREATE TABLE IF NOT EXISTS silver_session_features (
            user_id VARCHAR,
            platform_id INTEGER,
            session_start_ts TIMESTAMP,
            session_end_ts TIMESTAMP,
            session_date DATE,
            events_in_session INTEGER,
            had_view BOOLEAN,
            had_checkout BOOLEAN,
            had_purchase BOOLEAN,
            is_bounce BOOLEAN,
            is_abandoned_checkout BOOLEAN,
            cart_value_usd DECIMAL(12,6),
            cart_products VARCHAR[],
            is_high_value_abandoned_checkout BOOLEAN,
            PRIMARY KEY (user_id, platform_id, session_start_ts)
        );
    """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fct_product_campaigns (
            product_id VARCHAR NOT NULL,
            campaign_id VARCHAR NOT NULL,
            price DECIMAL(12,2) NOT NULL,
            discount_value DECIMAL(10,2) NOT NULL,

            total_events INTEGER NOT NULL,
            views INTEGER NOT NULL,
            checkouts INTEGER NOT NULL,
            purchases INTEGER NOT NULL,
            revenue DECIMAL(12,2) NOT NULL,

            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,

            PRIMARY KEY (product_id, campaign_id, price, discount_value)
        );
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_fpc_campaign ON fct_product_campaigns(campaign_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fpc_product ON fct_product_campaigns(product_id);")


    con.close()
    logging.info("Database setup complete. Connection closed.")


if __name__ == "__main__":
    logging.info("--- Starting Database Setup ---")
    setup_database()
    logging.info("--- Database Setup Finished ---")
