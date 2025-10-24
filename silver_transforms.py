import logging

import duckdb

DB_FILE = "nelo_analytics_rewrite.db"


def run_silver_transforms(
    con: duckdb.DuckDBPyConnection,
    logger: logging.Logger,
    session_minutes: int = 30,
    high_value_abandoned_checkout: int = 500,
) -> None:
    """
    Run idempotent Silver-layer transforms using a composite high-watermark on staging:
      - Build current_batch from stg_events via (ingestion_timestamp, event_id) HWM (no writes to staging)
      - Dedupe within the batch and anti-join to facts to form net_new
      - Upsert SCD dimensions, load facts, sessionize, and persist silver artifacts
    Assumes stg_events is append-only and fct_events dedupes by event_id.
    """

    # --- Composite HWM (ingestion_timestamp, event_id) ---------------------------------
    # Uses a stable upper bound captured at the start of the run to avoid race conditions
    # with new rows arriving in stg_events while Silver is executing.
    logger.info(" deriving current_batch from HWM...")

    con.execute(
        """
        -- 2) Capture stable bounds for this run into a temp table
        CREATE OR REPLACE TEMP TABLE silver_hwm AS
        WITH last AS (
            SELECT hwm_ts, hwm_event_id FROM etl_checkpoints WHERE pipeline = 'silver_events'
        ),
        ub AS (
            SELECT MAX(ingestion_timestamp) AS upper_ts FROM stg_events
        ),
        ub_eid AS (
            SELECT COALESCE(MAX(event_id), '') AS upper_eid
            FROM stg_events s
            JOIN ub ON s.ingestion_timestamp = ub.upper_ts
        )
        SELECT
            (SELECT hwm_ts       FROM last)     AS last_hwm_ts,
            (SELECT hwm_event_id FROM last)     AS last_hwm_event_id,
            (SELECT upper_ts     FROM ub)       AS upper_ts,
            (SELECT upper_eid    FROM ub_eid)   AS upper_eid;
        -- 3) Current batch = rows strictly after last HWM up to the captured upper bound
        CREATE OR REPLACE TEMP VIEW current_batch AS
        SELECT s.*
        FROM stg_events s
        CROSS JOIN silver_hwm h
        WHERE
            (h.upper_ts IS NOT NULL) -- no-op when staging is empty
        AND (
                (s.ingestion_timestamp >  h.last_hwm_ts AND s.ingestion_timestamp <  h.upper_ts)
            OR (s.ingestion_timestamp =  h.last_hwm_ts AND s.event_id >  h.last_hwm_event_id)
            OR (s.ingestion_timestamp =  h.upper_ts     AND s.event_id <= h.upper_eid)
        );
        -- 4) Deduplicate within the batch (keep latest by ingestion) and exclude events already in facts
        CREATE OR REPLACE TEMP TABLE net_new AS
        WITH batch_dedup AS (
            SELECT
                s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY ingestion_timestamp DESC
                ) AS rn
            FROM current_batch s
        ),
        deduped AS (
            SELECT * FROM batch_dedup WHERE rn = 1
        )
        SELECT d.*
        FROM deduped d
        LEFT JOIN fct_events f ON f.event_id = d.event_id
        WHERE f.event_id IS NULL;
    """
    )

    # Log batch sizes and bounds for observability
    try:
        hwm_row = con.execute(
            """
            SELECT last_hwm_ts, last_hwm_event_id, upper_ts, upper_eid
            FROM silver_hwm
            """
        ).fetchone()
        current_batch_count = con.execute(
            "SELECT COUNT(*) FROM current_batch"
        ).fetchone()[0]
        net_new_count = con.execute("SELECT COUNT(*) FROM net_new").fetchone()[0]
        logger.info(
            f"HWM bounds: last=({hwm_row[0]}, {hwm_row[1]}), upper=({hwm_row[2]}, {hwm_row[3]}); "
            f"current_batch_rows={current_batch_count}, net_new_rows={net_new_count}"
        )

    except Exception:
        # Non-fatal: continue without detailed HWM logs
        current_batch_count = None
        net_new_count = None

    # Users
    logger.info("Upserting into dim_users...")
    con.execute(
        """        
        -- Currently active SCD rows
        CREATE OR REPLACE TEMP VIEW current_users AS
        SELECT * FROM dim_users WHERE is_active = TRUE;

        -- Candidates from this batch
        CREATE OR REPLACE TEMP TABLE temp_user_changes AS 
        WITH user_first_event AS (
        SELECT user_id, MIN(DATE(event_timestamp)) AS first_event_date
        FROM net_new
        WHERE user_id IS NOT NULL
        GROUP BY user_id
        ),
        user_first_purchase AS (
        SELECT user_id, MIN(DATE(event_timestamp)) AS fp_date
        FROM net_new
        WHERE user_id IS NOT NULL AND event_name = 'purchase'
        GROUP BY user_id
        ),
        user_first_installment_purchase AS (
        SELECT user_id, MIN(DATE(event_timestamp)) AS inst_date
        FROM net_new
        WHERE user_id IS NOT NULL AND event_name = 'purchase' AND COALESCE(installments, 0) > 0
        GROUP BY user_id
        ),        

        -- Build potential changes (new, first/earlier purchase, first installments)
        raw_changes AS (
        -- New users
        SELECT
            u.user_id,
            u.first_event_date AS change_date,
            fp.fp_date AS new_first_purchase_date,
            CASE WHEN inst.inst_date IS NOT NULL THEN TRUE ELSE FALSE END AS new_has_used_installments
        FROM user_first_event u
        LEFT JOIN current_users c USING (user_id) 
        LEFT JOIN user_first_purchase fp USING (user_id)
        LEFT JOIN user_first_installment_purchase inst USING (user_id)
        WHERE c.user_id IS NULL

        UNION ALL

        -- First purchase discovered (or an earlier purchase arrives late)
        SELECT
            c.user_id,
            fp.fp_date AS change_date,
            fp.fp_date AS new_first_purchase_date,
            c.has_used_installments AS new_has_used_installments
        FROM current_users c
        JOIN user_first_purchase fp USING (user_id)
        WHERE fp.fp_date IS NOT NULL
            AND (c.first_purchase_date IS NULL OR fp.fp_date < c.first_purchase_date)

        UNION ALL

        -- First time using installments
        SELECT
            c.user_id,
            inst.inst_date AS change_date,
            c.first_purchase_date AS new_first_purchase_date,
            TRUE AS new_has_used_installments
        FROM current_users c
        JOIN user_first_installment_purchase inst USING (user_id)
        WHERE inst.inst_date IS NOT NULL
            AND c.has_used_installments = FALSE
        ),

        -- If multiple change types exist for a user in this batch, apply the earliest
        changes AS (
        SELECT
            user_id,
            MIN(change_date) AS change_date,
            MIN(new_first_purchase_date) FILTER (WHERE new_first_purchase_date IS NOT NULL) AS new_first_purchase_date,
            BOOL_OR(new_has_used_installments) AS new_has_used_installments
        FROM raw_changes
        GROUP BY user_id
        )
        SELECT * FROM changes;

        -- Same-day merge (Type 1 when change_date == start_date)
        UPDATE dim_users d
        SET
            first_purchase_date =
                CASE
                    WHEN ch.new_first_purchase_date IS NOT NULL
                        AND (d.first_purchase_date IS NULL
                            OR ch.new_first_purchase_date < d.first_purchase_date)
                    THEN ch.new_first_purchase_date
                    ELSE d.first_purchase_date
                END,
            has_used_installments =
                COALESCE(d.has_used_installments, FALSE)
                OR COALESCE(ch.new_has_used_installments, FALSE)
        FROM temp_user_changes ch
        WHERE d.user_id = ch.user_id
        AND d.is_active = TRUE
        AND d.effective_start_date = ch.change_date;   -- equality ⇒ in-place update

        -- Close interval that spans the change_date (handles open row and late-event splits)
        UPDATE dim_users d
        SET
            effective_end_date = ch.change_date,
            is_active = FALSE
        FROM temp_user_changes ch
        WHERE d.user_id = ch.user_id
            AND d.effective_start_date < ch.change_date
            AND (d.effective_end_date IS NULL OR ch.change_date < d.effective_end_date);

        -- Insert the new open version
        INSERT INTO dim_users (
            user_id,
            first_purchase_date,
            has_used_installments,
            effective_start_date,
            effective_end_date,
            is_active
        )
        SELECT
            ch.user_id,
            COALESCE(ch.new_first_purchase_date, c.first_purchase_date) AS first_purchase_date,
            COALESCE(ch.new_has_used_installments, c.has_used_installments, FALSE) AS has_used_installments,
            ch.change_date AS effective_start_date,
            NULL::DATE AS effective_end_date,
            TRUE AS is_active
        FROM temp_user_changes ch
        LEFT JOIN current_users c USING (user_id)
        LEFT JOIN dim_users du                   -- guard
            ON du.user_id = ch.user_id
            AND du.effective_start_date = ch.change_date
        WHERE du.user_id IS NULL;   

        DROP VIEW IF EXISTS current_users;
        DROP TABLE IF EXISTS temp_user_changes;
    """
    )
    logger.info("Successfully upserted into dim_users.")

    logger.info("Upserting into dim_products...")
    con.execute(
        """
        -- 1) Latest-per-day per product from net_new
        CREATE OR REPLACE TEMP VIEW product_day AS
        WITH ranked AS (
        SELECT
            product_id,
            DATE(event_timestamp) AS event_date,
            product_name,
            total_price AS price,
            event_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY product_id, DATE(event_timestamp)
            ORDER BY event_timestamp DESC
            ) AS rn
        FROM net_new
        WHERE product_id IS NOT NULL
        )
        SELECT product_id, event_date, product_name, price
        FROM ranked
        WHERE rn = 1;

        -- 2) Identify change points vs current active row
        CREATE OR REPLACE TEMP VIEW product_changes AS
        SELECT
        p.product_id,
        p.event_date AS change_date,
        p.product_name,
        p.price,
        d.effective_start_date,
        d.effective_end_date
        FROM product_day p
        LEFT JOIN dim_products d
        ON d.product_id = p.product_id
        AND d.is_active = TRUE
        WHERE d.product_id IS NULL
        OR d.product_name IS DISTINCT FROM p.product_name
        OR d.price IS DISTINCT FROM p.price;

        -- 3.a) Same-day merge (Type 1 update when change_date == effective_start_date)
        UPDATE dim_products d
        SET
            product_name = pc.product_name,
            price        = pc.price
        FROM product_changes pc
        WHERE d.product_id = pc.product_id
          AND d.is_active = TRUE
          AND d.effective_start_date = pc.change_date
          -- only when something actually changed
          AND (d.product_name IS DISTINCT FROM pc.product_name
               OR d.price IS DISTINCT FROM pc.price);

        -- 3.b) Close the interval that spans the change_date (open or historical)
        UPDATE dim_products d
        SET
            effective_end_date = pc.change_date,
            is_active         = FALSE         -- row being closed
        FROM product_changes pc
        WHERE d.product_id = pc.product_id
          AND d.effective_start_date < pc.change_date
          AND (d.effective_end_date IS NULL OR pc.change_date < d.effective_end_date);

        -- 3.c) Insert new open version (skip if a row already starts that day)
        INSERT INTO dim_products (
            product_id, product_name, price,
            effective_start_date, effective_end_date, is_active
        )
        SELECT
            pc.product_id,
            pc.product_name,
            pc.price,
            pc.change_date,
            NULL::DATE,
            TRUE
        FROM product_changes pc
        LEFT JOIN dim_products d2
          ON d2.product_id = pc.product_id
         AND d2.effective_start_date = pc.change_date   -- duplicate-start guard
        WHERE d2.product_id IS NULL;

        DROP VIEW IF EXISTS product_day;
        DROP VIEW IF EXISTS product_changes;

    """
    )
    logger.info("Successfully upserted into dim_products.")

    # Item lists - minimal SCD2 (attribute-only, no metrics in dim)
    logger.info("Upserting into dim_item_lists (SCD2)...")
    con.execute(
        """
        -- Latest name observed this batch (for Type 1 name maintenance)
        CREATE OR REPLACE TEMP VIEW name_latest AS
            SELECT item_list_id, item_list_name
            FROM (
                SELECT
                    item_list_id,
                    item_list_name,
                    event_timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY item_list_id
                        ORDER BY event_timestamp DESC
                    ) AS rn
                FROM net_new
                WHERE item_list_id IS NOT NULL
                  AND item_list_name IS NOT NULL
            ) t
            WHERE rn = 1;
        

        -- First date we ever saw each list in this batch
        WITH list_first AS (
            SELECT item_list_id, MIN(DATE(event_timestamp)) AS first_date
            FROM net_new
            WHERE item_list_id IS NOT NULL
            GROUP BY item_list_id
        ),
        current AS (
            SELECT * FROM dim_item_lists WHERE is_active = TRUE
        ),
        new_lists AS (
            SELECT
                lf.item_list_id,
                COALESCE(nl.item_list_name, lf.item_list_id) AS item_list_name,
                lf.first_date
            FROM list_first lf
            LEFT JOIN name_latest nl USING (item_list_id)
            LEFT JOIN current c USING (item_list_id)
            WHERE c.item_list_id IS NULL
        )

        -- Insert one open SCD row for never-seen lists
        INSERT INTO dim_item_lists (
            item_list_id, item_list_name, effective_start_date, effective_end_date, is_active
        )
        SELECT item_list_id, item_list_name, first_date, NULL, TRUE
        FROM new_lists;

        -- Type 1 name refresh (does not create SCD versions)
        UPDATE dim_item_lists d
        SET item_list_name = nl.item_list_name
        FROM name_latest nl
        WHERE d.item_list_id = nl.item_list_id
          AND d.is_active = TRUE
          AND nl.item_list_name IS DISTINCT FROM d.item_list_name;

        DROP VIEW IF EXISTS name_latest;
    """
    )
    logger.info("Successfully upserted into dim_item_lists (SCD2).")

    logger.info("Upserting into dim_campaigns (SCD2)...")
    con.execute(
        """
        -- Currently active SCD rows
        CREATE OR REPLACE TEMP VIEW current AS 
            SELECT * FROM dim_campaigns WHERE is_active = TRUE;

        CREATE OR REPLACE TEMP TABLE temp_changes AS
        WITH exploded AS (
            SELECT
                UNNEST(string_split(discount_campaigns, '|')) AS campaign_id,
                DATE(event_timestamp) AS event_date
            FROM net_new
            WHERE discount_campaigns IS NOT NULL
        ),
        first_last AS (
            SELECT
                campaign_id,
                MIN(event_date) AS first_date
            FROM exploded
            GROUP BY campaign_id
        ),
        changed AS (
            SELECT
                f.campaign_id,
                f.first_date AS change_date,
                f.campaign_id AS campaign_name
            FROM first_last f
            LEFT JOIN current c ON c.campaign_id = f.campaign_id
            WHERE c.campaign_id IS NULL
        )
        SELECT * FROM changed;


        -- Same-day merge  
        UPDATE dim_campaigns d
        SET
            campaign_name          = COALESCE(ch.campaign_name,          d.campaign_name),
        FROM temp_changes ch
        WHERE d.campaign_id           = ch.campaign_id
          AND d.is_active             = TRUE
          AND d.effective_start_date  = ch.change_date          -- equality ⇒ in-place
          AND d.campaign_name IS DISTINCT FROM ch.campaign_name;

        -- 1) Close any interval that spans the change date (late events etc.)
        UPDATE dim_campaigns d
        SET
            effective_end_date = ch.change_date,
            is_active          = FALSE
        FROM temp_changes ch
        WHERE d.campaign_id          = ch.campaign_id
          AND d.effective_start_date < ch.change_date
          AND (d.effective_end_date IS NULL OR ch.change_date < d.effective_end_date);

        -- 2) Insert the new open version (guard against duplicate start-dates)
        INSERT INTO dim_campaigns (
            campaign_id,
            campaign_name,
            targeting_source,
            targeting_segment,
            targeting_risk_segment,
            planned_start_date,
            planned_end_date,
            effective_start_date,
            effective_end_date,
            is_active
        )
        SELECT
            ch.campaign_id,
            ch.campaign_name,
            c.targeting_source,
            c.targeting_segment,
            c.targeting_risk_segment,
            c.planned_start_date,
            c.planned_end_date,
            ch.change_date,                     -- new version starts here
            NULL::DATE,
            TRUE
        FROM temp_changes ch
        LEFT JOIN current c
               ON c.campaign_id = ch.campaign_id
        LEFT JOIN dim_campaigns d2             -- duplicate-key guard
               ON d2.campaign_id        = ch.campaign_id
              AND d2.effective_start_date = ch.change_date
        WHERE d2.campaign_id IS NULL;          -- only insert if not present


        DROP VIEW IF EXISTS current;
        DROP TABLE IF EXISTS temp_changes;
    """
    )
    logger.info("Successfully upserted into dim_campaigns (SCD2).")

    logger.info("Upserting into dim_experiments (SCD2)...")
    con.execute(
        """
        -- Currently active SCD rows
        CREATE OR REPLACE TEMP VIEW current AS 
            SELECT * FROM dim_experiments WHERE is_active = TRUE;

        CREATE OR REPLACE TEMP TABLE temp_changes AS
        WITH events AS (
            SELECT
                discount_experiment_variant AS experiment_id,
                DATE(event_timestamp) AS event_date
            FROM net_new
            WHERE discount_experiment_variant IS NOT NULL
        ),
        first_dates AS (
            SELECT
                experiment_id,
                MIN(event_date) AS first_date
            FROM events
            GROUP BY experiment_id
        ),        
        changes AS (
            SELECT
                f.experiment_id,
                f.first_date AS change_date,
                f.experiment_id AS experiment_name,
                f.experiment_id AS variant_name
            FROM first_dates f
            LEFT JOIN current c ON c.experiment_id = f.experiment_id
            WHERE c.experiment_id IS NULL
        )
        SELECT * FROM changes;

        -- 0) Same-day merge  ────────────────────────────────────────────────
        -- If the incoming change_date equals the active row’s start date,
        -- update attributes in place instead of opening a new SCD version.
        UPDATE dim_experiments d
        SET
            experiment_name          = COALESCE(ch.experiment_name,          d.experiment_name),
            variant_name             = COALESCE(ch.variant_name,             d.variant_name)
        FROM temp_changes ch
        WHERE d.experiment_id           = ch.experiment_id
          AND d.is_active             = TRUE
          AND d.effective_start_date  = ch.change_date          -- equality ⇒ in-place
          AND (  d.experiment_name          IS DISTINCT FROM ch.experiment_name
              OR d.variant_name             IS DISTINCT FROM ch.variant_name );

        -- 1) Close any interval that spans the change date (late events etc.)
        UPDATE dim_experiments d
        SET
            effective_end_date = ch.change_date,
            is_active          = FALSE
        FROM temp_changes ch
        WHERE d.experiment_id          = ch.experiment_id
          AND d.effective_start_date < ch.change_date
          AND (d.effective_end_date IS NULL OR ch.change_date < d.effective_end_date);

        -- 2) Insert the new open version (guard against duplicate start-dates)
        INSERT INTO dim_experiments (
            experiment_id,
            experiment_name,
            variant_name,
            targeting_source,
            targeting_segment,
            targeting_risk_segment,
            planned_start_date,
            planned_end_date,
            effective_start_date,
            effective_end_date,
            is_active
        )
        SELECT
            ch.experiment_id,
            ch.experiment_name,
            ch.variant_name,
            c.targeting_source,
            c.targeting_segment,
            c.targeting_risk_segment,
            c.planned_start_date,
            c.planned_end_date,
            ch.change_date,                     -- new version starts here
            NULL::DATE,
            TRUE
        FROM temp_changes ch
        LEFT JOIN current c
               ON c.experiment_id = ch.experiment_id
        LEFT JOIN dim_experiments d2             -- duplicate-key guard
               ON d2.experiment_id        = ch.experiment_id
              AND d2.effective_start_date = ch.change_date
        WHERE d2.experiment_id IS NULL;          -- only insert if not present


        DROP VIEW IF EXISTS current;
        DROP TABLE IF EXISTS temp_changes;
    """
    )
    logger.info("Successfully upserted into dim_experiments (SCD2).")

    logger.info("Upserting into fct_product_campaigns (incremental, from net_new)...")
    con.execute(
        """
        WITH exploded AS (
            SELECT
                DATE(e.event_timestamp) AS date,
                e.product_id,
                UNNEST(string_split(e.discount_campaigns, '|')) AS campaign_id,
                e.event_name,
                COALESCE(e.discount_value, 0) AS discount_value,
                e.item_revenue
            FROM net_new e
            WHERE e.product_id IS NOT NULL
            AND e.discount_campaigns IS NOT NULL AND e.discount_campaigns <> ''
        ),
        campaigns AS (
            SELECT ex.*
            FROM exploded ex
            JOIN dim_campaigns c
            ON c.campaign_id = ex.campaign_id
            AND c.effective_start_date <= ex.date
            AND (c.effective_end_date IS NULL OR ex.date < c.effective_end_date)
        ),
        products AS (
            SELECT ca.*, p.price
            FROM campaigns ca
            JOIN dim_products p
            ON p.product_id = ca.product_id
            AND p.effective_start_date <= ca.date
            AND (p.effective_end_date IS NULL OR ca.date < p.effective_end_date)
        ),
        agg AS (
            SELECT
                product_id,
                campaign_id,
                price,
                discount_value,
                COUNT(*) AS total_events,
                COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) AS views,
                COUNT(CASE WHEN event_name = 'begin_checkout' THEN 1 END) AS checkouts,
                COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS purchases,
                SUM(CASE WHEN event_name = 'purchase' THEN item_revenue ELSE 0 END) AS revenue,
                MIN(date) AS first_seen_date,
                MAX(date) AS last_seen_date
            FROM products
            GROUP BY product_id, campaign_id, price, discount_value
        )
        INSERT INTO fct_product_campaigns (
            product_id, campaign_id, price, discount_value,
            total_events, views, checkouts, purchases, revenue,
            first_seen_date, last_seen_date
        )
        SELECT
            product_id, campaign_id, price, discount_value,
            total_events, views, checkouts, purchases, revenue,
            first_seen_date, last_seen_date
        FROM agg
        ON CONFLICT (product_id, campaign_id, price, discount_value) DO UPDATE SET
            total_events = fct_product_campaigns.total_events + EXCLUDED.total_events,
            views        = fct_product_campaigns.views + EXCLUDED.views,
            checkouts    = fct_product_campaigns.checkouts + EXCLUDED.checkouts,
            purchases    = fct_product_campaigns.purchases + EXCLUDED.purchases,
            revenue  = fct_product_campaigns.revenue + EXCLUDED.revenue,
            first_seen_date = LEAST(fct_product_campaigns.first_seen_date, EXCLUDED.first_seen_date),
            last_seen_date  = GREATEST(fct_product_campaigns.last_seen_date, EXCLUDED.last_seen_date);

        """
    )
    logger.info("Successfully upserted fct_product_campaigns.")

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
            ingestion_timestamp,
            updated_at
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
            now() as ingestion_timestamp,
            now() as updated_at

        FROM net_new stg
        LEFT JOIN dim_platforms p ON UPPER(stg.platform) = p.platform_name
        ON CONFLICT (event_id) DO NOTHING;
    """
    )
    logger.info("Successfully loaded into fct_events.")

    logger.info("Upserting into fct_product_daily_performance...")
    con.execute(
        """
        WITH daily_agg AS (
            SELECT
                CAST(strftime('%Y%m%d', event_timestamp) AS INTEGER) AS date_id,
                product_id,
                COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) AS views,
                COUNT(CASE WHEN event_name = 'begin_checkout' THEN 1 END) AS checkouts,
                COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS purchases,
                SUM(CASE WHEN event_name = 'purchase' THEN item_revenue ELSE 0 END) AS revenue
            FROM net_new
            WHERE product_id IS NOT NULL
            GROUP BY 1, 2
        )
        INSERT INTO fct_product_daily_performance (
            date_id, product_id, views, checkouts, purchases, revenue
        )
        SELECT date_id, product_id, views, checkouts, purchases, revenue
        FROM daily_agg
        ON CONFLICT(date_id, product_id) DO UPDATE SET
            views     = fct_product_daily_performance.views + EXCLUDED.views,
            checkouts = fct_product_daily_performance.checkouts + EXCLUDED.checkouts,
            purchases = fct_product_daily_performance.purchases + EXCLUDED.purchases,
            revenue   = fct_product_daily_performance.revenue + EXCLUDED.revenue;
    """
    )
    logger.info("Successfully updated fct_product_daily_performance.")

    logger.info("Sessionizing events and preparing list impressions...")
    # 0) Bring in pre-history equal to the session window to bridge batch boundaries
    con.execute(
        f"""
        -- Identify impacted actors to scope prehistory, and compute batch lower bound
        CREATE OR REPLACE TEMP VIEW impacted AS
        SELECT DISTINCT user_id, UPPER(platform) AS platform_name
        FROM net_new
        WHERE user_id IS NOT NULL;

        CREATE OR REPLACE TEMP VIEW bounds AS
        SELECT MIN(event_timestamp) AS min_ts FROM net_new;

        -- Prehistory strictly before this batch's earliest event, within the session window
        CREATE OR REPLACE TEMP VIEW prehistory AS
        SELECT
          fe.event_id,
          fe.event_name,
          fe.event_timestamp,
          fe.user_id,
          p.platform_name
        FROM fct_events fe
        JOIN dim_platforms p USING (platform_id)
        JOIN impacted i
          ON i.user_id = fe.user_id
         AND i.platform_name = p.platform_name
        JOIN bounds b
          ON fe.event_timestamp >= b.min_ts - INTERVAL '{session_minutes} minutes'
         AND fe.event_timestamp <  b.min_ts
        LEFT JOIN net_new n ON n.event_id = fe.event_id   -- belt-and-suspenders
        WHERE n.event_id IS NULL;

        -- Final window: current batch + just enough prehistory to split sessions
        CREATE OR REPLACE TEMP VIEW events_window AS
        SELECT
          event_id, event_name, event_timestamp, user_id, UPPER(platform) AS platform_name
        FROM net_new
        UNION ALL
        SELECT
          event_id, event_name, event_timestamp, user_id, platform_name
        FROM prehistory;
        """
    )

    # 1) Sessionize per (user_id, platform_id) with configured inactivity window
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE sessionized_events AS
        WITH base AS (
          SELECT ew.event_id, ew.event_name, ew.event_timestamp, ew.user_id, p.platform_id
          FROM events_window ew
          LEFT JOIN dim_platforms p ON p.platform_name = ew.platform_name
        ),
        ordered AS (
          SELECT
            *,
            LAG(event_timestamp) OVER (
              PARTITION BY user_id, platform_id
              ORDER BY event_timestamp, event_id
            ) AS prev_ts
          FROM base
        ),
        marked AS (
          SELECT
            *,
            CASE
              WHEN prev_ts IS NULL OR event_timestamp - prev_ts > INTERVAL '{session_minutes} minutes'
              THEN 1 ELSE 0
            END AS is_new_session
          FROM ordered
        ),
        numbered AS (
          SELECT
            *,
            SUM(is_new_session) OVER (
              PARTITION BY user_id, platform_id
              ORDER BY event_timestamp, event_id
              ROWS UNBOUNDED PRECEDING
            ) AS sess_seq
          FROM marked
        )
        SELECT
          event_id, event_name, event_timestamp, user_id, platform_id,
          MIN(event_timestamp) OVER (PARTITION BY user_id, platform_id, sess_seq) AS session_start_ts,
          CAST(MIN(event_timestamp) OVER (PARTITION BY user_id, platform_id, sess_seq) AS DATE) AS session_date,
          user_id || ':' || CAST(platform_id AS VARCHAR) || ':' ||
          CAST(MIN(event_timestamp) OVER (PARTITION BY user_id, platform_id, sess_seq) AS VARCHAR) AS session_id
        FROM numbered;
        """
    )
    # Debug counts after sessionization
    try:
        sessionized_cnt = con.execute(
            "SELECT COUNT(*) FROM sessionized_events"
        ).fetchone()[0]
        # Count distinct session groups (by grouping key)
        session_groups_cnt = con.execute(
            """
            WITH session_groups AS (
              SELECT user_id, platform_id, session_start_ts
              FROM sessionized_events
              GROUP BY user_id, platform_id, session_start_ts
            )
            SELECT COUNT(*) FROM session_groups
            """
        ).fetchone()[0]
        logger.info(
            f"Debug counts: sessionized_events_rows={sessionized_cnt}, session_groups={session_groups_cnt}"
        )
    except Exception as e:
        logger.info(f"Debug counts (sessionized) unavailable: {e}")

    # 2) Materialize impressions (current batch + pre-history within the window)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW list_impressions_source AS
        SELECT
          n.event_id,          
          n.user_id,
          p.platform_id,
          n.item_list_id,
          n.event_timestamp AS impression_ts
        FROM net_new n
        LEFT JOIN dim_platforms p ON UPPER(n.platform) = p.platform_name
        WHERE n.event_name = 'view_item_list' AND n.item_list_id IS NOT NULL

        UNION ALL

        SELECT
          fe.event_id,
          fe.user_id,
          fe.platform_id,
          fe.item_list_id,
          fe.event_timestamp AS impression_ts
        FROM fct_events fe
        WHERE fe.event_name = 'view_item_list'
          AND fe.item_list_id IS NOT NULL
          AND fe.event_timestamp >= (
            SELECT min_ts - INTERVAL '{session_minutes} minutes' FROM bounds
          )
          AND fe.event_timestamp < (
            SELECT min_ts FROM bounds
          );
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE list_impressions AS
        SELECT
          li.event_id,
          li.user_id,
          li.platform_id,
          li.item_list_id,
          li.impression_ts,
          se.session_start_ts,
          se.session_date,
          se.session_id
        FROM list_impressions_source li
        JOIN sessionized_events se
          ON se.event_id = li.event_id
        """
    )

    # 3) Last-touch list attribution within the session
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE attributed_events AS
        SELECT
          se.event_id,
          se.user_id,
          se.platform_id,
          se.event_name,
          se.event_timestamp,
          se.session_id,
          se.session_start_ts,
          se.session_date,
          (
            SELECT li.item_list_id
            FROM list_impressions li
            WHERE li.user_id = se.user_id
              AND li.platform_id = se.platform_id
              AND li.session_id = se.session_id
              AND li.impression_ts <= se.event_timestamp
            ORDER BY li.impression_ts DESC, li.event_id DESC
            LIMIT 1
          ) AS item_list_id,
          (
            SELECT li.impression_ts
            FROM list_impressions li
            WHERE li.user_id = se.user_id
              AND li.platform_id = se.platform_id
              AND li.session_id = se.session_id
              AND li.impression_ts <= se.event_timestamp
            ORDER BY li.impression_ts DESC, li.event_id DESC
            LIMIT 1
          ) AS impression_ts_chosen
        FROM sessionized_events se
        JOIN net_new n ON se.event_id = n.event_id
        WHERE n.event_name IN ('view_item','begin_checkout','purchase') AND n.user_id IS NOT NULL;
        """
    )

    # 4) Persist list impressions to silver (idempotent)
    logger.info("Persisting silver list impressions...")
    con.execute(
        """
        INSERT INTO silver_list_impressions (
          user_id, platform_id, session_id, session_start_ts, session_date,
          item_list_id, impression_ts, date_id
        )
        SELECT DISTINCT
          user_id, platform_id, session_id, session_start_ts, session_date,
          item_list_id, impression_ts, DATE(impression_ts)
        FROM list_impressions
        ON CONFLICT DO NOTHING;
        """
    )

    # Persist event-level list attribution for Gold (sessionized, last-touch)
    logger.info("Persisting silver list attribution...")
    con.execute(
        f"""
        INSERT INTO silver_event_list_attribution (
            event_id,
            user_id,
            event_name,
            event_timestamp,
            item_list_id,
            item_revenue_in_usd,
            installments,
            date_id,
            platform_id,
            session_id,
            impression_ts_chosen,
            attribution_model,
            window_minutes,
            attribution_version
        )
        SELECT
            ae.event_id,
            ae.user_id,
            ae.event_name,
            ae.event_timestamp,
            ae.item_list_id,
            n.item_revenue_in_usd,
            n.installments,
            DATE(ae.event_timestamp) AS date_id,
            ae.platform_id,
            ae.session_id,
            ae.impression_ts_chosen,
            'last_touch_session' AS attribution_model,
            {session_minutes} AS window_minutes,
            1 AS attribution_version
        FROM attributed_events ae
        JOIN net_new n ON n.event_id = ae.event_id
        WHERE ae.item_list_id IS NOT NULL
        ON CONFLICT (event_id) DO NOTHING;
        """
    )
    logger.info("Successfully persisted silver list attribution.")

    logger.info("Persisting silver session campaign last-touch attribution...")
    con.execute(
        """
        -- 1. Explode campaign touches from the current batch
        WITH campaign_events AS (
            SELECT
                event_id,
                event_timestamp,
                UNNEST(string_split(discount_campaigns, '|')) AS campaign_id
            FROM net_new
            WHERE discount_campaigns IS NOT NULL AND discount_campaigns <> ''
        ),
        -- 2. Join with session data and find the last touch for each session
        last_touch_per_session AS (
            SELECT
                se.user_id,
                se.platform_id,
                se.session_id,
                se.session_start_ts,
                ce.campaign_id,
                ce.event_timestamp AS last_touch_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY se.session_id
                    ORDER BY ce.event_timestamp DESC, ce.event_id DESC -- Tie-break for determinism
                ) as rn
            FROM sessionized_events se
            JOIN campaign_events ce ON ce.event_id = se.event_id
        )
        -- 3. Insert the winning last-touch record for each session
        INSERT INTO silver_session_campaign_last_touch (
            user_id,
            platform_id,
            session_id,
            session_start_ts,
            campaign_id,
            last_touch_ts,
            updated_at
        )
        SELECT
            user_id,
            platform_id,
            session_id,
            session_start_ts,
            campaign_id,
            last_touch_ts,
            CURRENT_TIMESTAMP AS updated_at
        FROM last_touch_per_session
        WHERE rn = 1
        ON CONFLICT (user_id, platform_id, session_id) DO UPDATE SET
            campaign_id   = EXCLUDED.campaign_id,
            last_touch_ts = EXCLUDED.last_touch_ts,
            updated_at    = EXCLUDED.updated_at;
        """
    )
    logger.info(
        "Successfully persisted silver session campaign last-touch attribution."
    )

    # Advance the ETL checkpoint to this run's captured upper bound (if any)
    logger.info("Advancing ETL checkpoint to this run's upper bound...")

    # Persist checkpoint and batch counts
    con.execute(
        """
        UPDATE etl_checkpoints
        SET
            hwm_ts = (SELECT MAX(event_timestamp) FROM net_new),
            hwm_event_id = (SELECT event_id FROM net_new ORDER BY event_timestamp DESC, event_id DESC LIMIT 1),
            last_run_current_batch_rows = ?,
            last_run_net_new_rows = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE pipeline = 'silver_events'
        """,
        (current_batch_count, net_new_count),
    )

    # Final observability: how many staging rows were processed this run
    try:
        if current_batch_count is None:
            current_batch_count = con.execute(
                "SELECT COUNT(*) FROM current_batch"
            ).fetchone()[0]
        logger.info(f"Silver processed {current_batch_count} staging rows in this run.")
    except Exception:
        pass

    logger.info("Creating silver purchases view...")
    con.execute(
        """
        -- Finance purchases (view; event grain)
            CREATE OR REPLACE VIEW silver_purchases AS
            SELECT
                f.event_id,
                f.user_id,
                f.platform_id,
                f.product_id,
                f.item_list_id,
                f.item_revenue,
                f.installments,
                f.installment_price,
                f.total_price,
                f.price,
                f.discount_value,
                f.quantity,
                f.event_timestamp,
                DATE(f.event_timestamp) AS event_date
            FROM fct_events f
            WHERE f.event_name = 'purchase';
        """
    )
    logger.info("Successfully created silver list impressions view.")

    logger.info("Sessionized events in Silver Layer")
    con.execute(
        """
        -- A) Aggregate sessions from sessionized_events once per batch
        WITH session_agg AS (
        SELECT
            user_id,
            platform_id,
            MIN(event_timestamp) AS session_start_ts,
            MAX(event_timestamp) AS session_end_ts,
            CAST(MIN(event_timestamp) AS DATE) AS session_date,
            COUNT(*) AS events_in_session,
            COUNT(CASE WHEN event_name = 'view_item' THEN 1 END) AS views,
            COUNT(CASE WHEN event_name = 'begin_checkout' THEN 1 END) AS checkouts,
            COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS purchases
        FROM sessionized_events
        GROUP BY user_id, platform_id,
                -- the same session grouping you used to derive session_id/session_start_ts
                session_start_ts
        )
        INSERT INTO silver_user_sessions (
          user_id, platform_id, session_start_ts, session_end_ts, session_date,
          events_in_session, views, checkouts, purchases
        )
        SELECT 
            user_id,
            platform_id,
            session_start_ts,
            session_end_ts,
            session_date,
            events_in_session,
            views,
            checkouts,
            purchases
        FROM session_agg
        ON CONFLICT (user_id, platform_id, session_start_ts) DO UPDATE SET
            session_end_ts = EXCLUDED.session_end_ts,
            session_date = EXCLUDED.session_date,
            events_in_session = EXCLUDED.events_in_session,
            views = EXCLUDED.views,
            checkouts = EXCLUDED.checkouts,
            purchases = EXCLUDED.purchases;
    """
    )
    logger.info("Successfully created silver sessionized events view.")

    logger.info(
        "Building features and cart metrics per session in silver_session_features"
    )
    # -- B) Build features and cart metrics per session (single pass join to fct_events)
    con.execute(
        f"""
        WITH cart AS (
            WITH checkout_lines AS (
                SELECT DISTINCT
                s.user_id, s.platform_id, s.session_start_ts,
                fe.event_id, fe.product_id,
                fe.price * fe.quantity AS line_value
                FROM silver_user_sessions s
                JOIN fct_events fe
                    ON fe.user_id = s.user_id
                    AND fe.platform_id = s.platform_id
                    AND fe.event_timestamp BETWEEN s.session_start_ts AND s.session_end_ts
                WHERE fe.event_name = 'begin_checkout'
            )
            SELECT
                user_id, platform_id, session_start_ts,
                SUM(line_value) AS cart_value,
                LIST(DISTINCT product_id) AS cart_products
            FROM checkout_lines
            GROUP BY user_id, platform_id, session_start_ts
        )
        INSERT INTO silver_session_features (
            user_id, platform_id, session_start_ts, session_end_ts, session_date,
            events_in_session, had_view, had_checkout, had_purchase,
            is_bounce, is_abandoned_checkout, cart_value, cart_products,
            is_high_value_abandoned_checkout
        )
        SELECT
            s.user_id, s.platform_id, s.session_start_ts, s.session_end_ts, s.session_date,
            s.events_in_session,
            (s.views > 0)     AS had_view,
            (s.checkouts > 0) AS had_checkout,
            (s.purchases > 0) AS had_purchase,
            (s.events_in_session = 1)                 AS is_bounce,
            (s.checkouts > 0 AND s.purchases = 0)     AS is_abandoned_checkout,
            COALESCE(c.cart_value, 0)             AS cart_value,
            c.cart_products,
            (s.checkouts > 0 AND s.purchases = 0 AND COALESCE(c.cart_value,0) > {high_value_abandoned_checkout})
                                                    AS is_high_value_abandoned_checkout
        FROM silver_user_sessions s
        LEFT JOIN cart c
            ON c.user_id = s.user_id
            AND c.platform_id = s.platform_id
            AND c.session_start_ts = s.session_start_ts
        ON CONFLICT (user_id, platform_id, session_start_ts) DO UPDATE SET
            session_end_ts = EXCLUDED.session_end_ts,
            session_date = EXCLUDED.session_date,
            events_in_session = EXCLUDED.events_in_session,
            had_view = EXCLUDED.had_view,
            had_checkout = EXCLUDED.had_checkout,
            had_purchase = EXCLUDED.had_purchase,
            is_bounce = EXCLUDED.is_bounce,
            is_abandoned_checkout = EXCLUDED.is_abandoned_checkout,
            cart_value = EXCLUDED.cart_value,
            cart_products = EXCLUDED.cart_products,
            is_high_value_abandoned_checkout = EXCLUDED.is_high_value_abandoned_checkout;
        """
    )
    logger.info("Successfully created silver sessionized events view.")
