# Nelo Growth Analytics Data Pipeline: A Case Study

## 1. Overview & Business Objectives

This document details the architecture and design of the Nelo Growth Analytics data pipeline. The pipeline is designed to answer critical business questions related to customer segmentation and marketing effectiveness.

### Key Business Questions:

*   **User Engagement:**
    *   What are the core user engagement funnels (e.g., view -> checkout -> purchase)? Where are the biggest drop-offs?
    *   What is the bounce rate for user sessions?
*   **Marketing & Campaign Effectiveness:**
    *   What is the conversion rate from product view to purchase?
    *   What is the Average Order Value for users who abandoned cart
    *   Can we target users who dropped off after checkout and has no purchase since?
    *   How does platform (e.g., iOS, Android, Web) item lists and campaigns compare in terms of users, revenue, and engagement?
    *   How do different marketing campaigns perform in terms of conversion and revenue?
    *   How do experiments and coupons impact user behavior and key metrics like Average Order Value (AOV)?
*   **Financial Metrics:**
    *   Which products changed in price during the course of our current campaigns?
    *   What are the discounts provided at a product and campaign level, as well as daily purchase level data?
    *   What is the adoption rate of our installment payment plans?


## 2. System Architecture Diagrams

### Current Architecture (Single-Node, DuckDB)
![Current Architecture](plan/Current_Arch.png)

### 10x Scale Architecture (Distributed, AWS S3, Spark Clusters, Athena)
![Prod Architecture](plan/Prod_Arch.png)

### Medallion Data Schema Design
![Data Flow Diagram](plan/Data_Flow_Diagram.png)

## 3. Architecture Decisions (snapshot)

| # | Decision | Rationale  | 
|:-:|----------|-------------------------|
| 1 | **DuckDB single-node** for case-study                  | zero-setup, SQL-compatible, vectorised |
| 2 | **File-based micro-batch ingestion** from SQS                 | idempotent, cheap, good enough for 5-min latency |
| 3 | **Sessions as analytic grain** for CTR / funnels        | avoids event-level noise, matches Growth questions |
| 4 | **Click attribution: last-touch + 30 min window**       | simple, mimics GA4, performant on sample size |
| 6 | **Silver = Transformations Layer** Pre-aggregations          | speed up analytics downstream, incremental refresh  |
| 5 | **Gold = Views** instead of materialised tables         | instant freshness, no extra storage, semantic separation |
| 6 | **Config.json** Config driven development         | malleable to requirement changes |
| 7 | **Partitioning / Parquet lakehouse** when > 100 k events/batch | keeps scans bounded, S3-friendly |


## 4. How the design answers the business questions

*   **Which lists engage users the most?** Use `vw_list_daily_performance_sessions` for exposures, session-level clicks/purchases, CTR and conversion rates.
*   **Top e-commerce metrics:** CTR, bounce rate, funnel rates, conversion rates, AOV, and installment adoption are available via `vw_platform_daily_performance_sessions` and session features.

## 5. Pipeline Architecture & Data Flow

The pipeline follows the multi-layered **Medallion Architecture** (Raw -> Staging -> Silver -> Gold) to ensure data quality, scalability, and modularity.
Silver layer takes care of transformations and pre-aggregations whereas Gold is used as a semantic layer to customize views for different teams. 
For example, Finance does not need access to engagement metrics of Growth teams.

**Orchestration:** The entire pipeline is orchestrated with Python scripts, using DuckDB as the analytical engine (for the purpose of this case study, DuckDB allows for easy setup and experimentation). When considering scale, Python and can be easily converted to PySpark and SparkSQL whereas we can store the processed data in parquet files on s3 consumed by Athena for faster processing. All this can be orchestrated using Airflow.

### Technologies & Rationale

*   Python + DuckDB: Fast local analytics with vectorized execution and transactional semantics without external dependencies.
*   boto3 + SQS: Simple polling ingestion from the assignment-provided queue.
*   diskcache: Lightweight on-disk cache to track seen SQS MessageIds and mitigate duplicates during a run. Frees up downstream DB layer compute. 
*   Pandas: Robust JSONL parsing and schema coercion before bulk-loading to DuckDB. 
*   SQL-first transforms: Star-schema Silver with SCD2 dims and facts; Gold as lightweight semantic views on Silver for immediate analytics.

### Step 1: Ingestion (`etl.py`)

*   **Source:** Events are consumed from an AWS SQS queue using credentials from the environment.
*   **At-least-once handling & dedup guard:** Due to limited permissions (no `DeleteMessage`), ingestion operates in at-least-once mode. A `diskcache`-backed `seen_msg_cache` tracks `MessageId`s during the run to avoid immediate duplicates; durable idempotency is enforced downstream in SQL.
*   **Batching:** Messages are appended to `raw_events.jsonl` and then atomically renamed to a unique, immutable batch file (e.g., `processing_batch_YYYYMMDD_HHMMSS.jsonl`). The batch is copied to `staging/` for processing and archived to `archive/` to support reprocessing and audits.
*   **Trigger:** After a successful consume, the batch processing script `process_staging.py` is executed. Currently handles 10k rows in 15 secs but for the purpose of costs and evaluating the need for realtime data, the job can be run every 15 mins. 
*   **Config:** To allow for changes as per future technical and business needs, various parameters such as 'source', 'destination' 'staging', 'dead_letter_queue' locations as well as query params for business logic is currently stored in a `config.json` file.

### Step 2: Staging & Processing (`process_staging.py`)

*   **Parsing & Validation:** Reads each JSONL line and safely parses into events. A targeted GA4-style `items` parser expands the stringified `items` payload into a list of item dicts. Validation checks required fields (`event_id` is deterministically generated, `event_name`, `event_timestamp`). Parse/validate failures are written to a DLQ file (`dead_letter/process_staging_failures_{RUN_TS}.jsonl`) so one bad event never breaks the batch.
*   **Flattening:** Unnests `items` so each row captures one user×product interaction with extracted fields (e.g., list id/name, price, quantity, discount value, discounts/campaign codes, installments, in-stock).
*   **Loading:** Bulk-loads processed rows into append-only `stg_events` (with a `processed_at` flag). Uses vectorized DataFrame registration; on per-row failures, falls back to row-wise inserts and records DLQ context. 
*   **Transactional Control:** The Staging load and subsequent Silver transforms run in a single database transaction for atomicity. On success, the file is moved to `processed/`. Gold is defined as semantic views (no separate transaction required).

### Step 3: Silver Layer (`silver_transforms.py`)

*   **Objective:** To clean, conform, and model the data into a star schema.
*   **Idempotency:** Creates a `current_batch` and `net_new` view: dedupes within-batch, then anti-joins to `fct_events` by `event_id`. This isolates unprocessed rows and prevents duplicates across retries. Marked staged rows as processed.
*   **Dimension updates (SCD2 with same-day merge when applicable):**
    *   `dim_users`: Tracks `first_purchase_date` and `has_used_installments`; closes/open new versions when changes occur; same-day changes apply in-place.
    *   `dim_products`: SCD2 on `product_name` and `price` changes; maintains current active version per product.
    *   `dim_item_lists`: Minimal SCD2; inserts unseen lists and Type 1 refresh of list names.
    *   `dim_campaigns` and `dim_experiments`: SCD2 derived from discount codes/variants observed in events; preserve active windows for accurate point-in-time joins.
    *   Certain SCDs seem unnecessary and same-day merge code is not optimized, will migrate as and when required based on query performance. 
*   **Facts:**
    *   `fct_events`: Immutable event grain with pricing, discounts, campaign codes, experiment id, installments, platform/date keys; `ON CONFLICT DO NOTHING` ensures idempotence. 
    *   `fct_product_campaigns`: Incremental aggregate by `(product_id, campaign_id, price, discount_value)` with additive upserts of views/checkouts/purchases/revenue and min/max seen dates.
*   **Performance Tuning:**
    * `partitioning`: DuckDB doesnt support partitioning, so currently I've implemented indexes. Ideal to partition events data by date for best performance.

*   **Sessions & list attribution (Silver persisted):** Builds sessions per `(user_id, platform_id)` using a 30-minute activity window; materializes `silver_list_impressions` (sessionized list exposures) and `silver_event_list_attribution` (last-touch list per event within session, with purchase revenue and installments for downstream analytics). Persists `silver_user_sessions` and `silver_session_features` (bounce, abandon, cart value/products, high-value abandon flag). 

### Step 4: Gold Layer (`gold_transforms.py`)

*   **Objective:** Provide analyst-ready semantic layers without additional storage/refresh complexity. Different stakeholders / teams get access to different views of the underlying data.
*   **View-driven semantics:** Gold is expressed as views on Silver/Facts, ensuring up-to-date results as soon as Silver commits. Key views:
    *   `vw_gold_product_campaign_price_change_events`: Joins SCD2 product price change points to active campaigns to analyze price deltas during campaign windows.
    *   `vw_gold_product_campaign_current_status`: For each active `(campaign, product)`, shows current price and discount flags (today and historical) leveraging compact `fct_product_campaigns`.
    *   `vw_growth_engagement_daily`: Session-based list exposures, clicks, purchases, and rates computed from `silver_list_impressions` and `silver_event_list_attribution`, joined on canonical session and optionally enriched with last-touch campaign.
    *   User segments for campaign targetting:
        *   `gold_segment_bounce_v`: Sessions that bounced.
        *   `gold_segment_checkout_abandon_v`: Sessions that abandoned checkout.
        *   `gold_segment_high_value_abandon_v`: High-value checkout abandons.
        *   `gold_segment_abandon_then_bought_different_v`: Users who abandoned a cart and later purchased a different product.

### Data Consistency & Latency

*   Ingestion is at-least-once; durable idempotency is enforced at the database via `event_id` and `ON CONFLICT` logic.
*   Staging + Silver run in a single transaction per file: either both commit, or neither—ensuring atomic dimensional consistency and facts.
*   Gold views read directly from committed Silver/Facts, providing near-immediate query freshness after Silver commit without separate refresh jobs.

### Trade-offs & Design Decisions

*   File-based batches plus atomic rename provide simplicity and reprocessability; upstream queue deletes are out of scope due to permissions.
*   Gold-as-views avoids maintaining additional state; if materialized tables are needed later, the SQL can be reused with a watermarking pattern.
*   Last-touch attribution within sessions uses a correlated lookup over sessionized impressions; it is correct and simple but can become the main hotspot at much larger scale (see Roadmap).


## 6. Data Modeling: The Bus Matrix

I employed a dimensional modeling approach to structure our data. The "Bus Matrix" below maps the core business processes to the dimensions and facts (metrics) that describe them. It serves as the blueprint for our Silver and Gold data layers.

| Business Process | User | Product | Campaign | Platform | Facts / Metrics |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **User Engagement** | ✅ | ✅ | | ✅ | `sessions`, `unique_users`, `bounce_rate`, `total_events` |
| **Checkout Funnel** | ✅ | ✅ | | ✅ | `sessions_with_view`, `sessions_with_checkout`, `sessions_with_purchase`, `view_to_checkout_rate_session`, `checkout_to_purchase_rate_session`, `overall_session_conversion_rate` |
| **List Performance** | ✅ | ✅ | | ✅ | `list_impressions`, `sessions_exposed`, `sessions_with_click`, `ctr_sessions`, `purchase_rate_on_exposure`, `purchase_rate_on_click` |
| **Campaign Perf.** | ✅ | | ✅ | ✅ | `sessions_exposed`, `sessions_with_purchase`, `session_conversion_rate`, `total_revenue` |

*   **Silver Layer:** Implements this model with a star schema, consisting of a central `fct_events` table and surrounding dimension tables (`dim_users`, `dim_products`, etc.).
*   **Gold Layer:** Provides semantic views over Silver/Facts optimized for BI (e.g., `vw_platform_daily_performance_sessions`, `vw_growth_engagement_daily`).

Note: CTR and conversion metrics are computed at the session grain (not per-event), powered by `silver_session_features`, `silver_list_impressions`, and `silver_event_list_attribution`.


## 7. Scalability & Limitations

The pipeline is designed to be highly scalable within the constraints of a single-node architecture.

### Current State (Processing ~16k events/batch):

*   **Performance:** The pipeline is extremely fast. DuckDB's vectorized, in-process engine excels at this scale. Incremental processing in both Silver and Gold layers means that batch processing times are consistently low.

### Future Scale (Processing ~100k+ events/batch):

*   **Gold Layer:** View-driven semantics keep the refresh cost near-zero; heavy lifting remains in Silver. If persisted marts are needed, adopt date-partitioned, watermark-based delete/insert materialization.
*   **Silver Layer:** Mostly ready. Batch SCD2 upserts and fact inserts scale linearly. The session last-touch attribution uses a correlated subquery that can become the bottleneck at 10x+ scale.
*   **Ingestion/Staging:** The file-based batching and Python-driven parsing will eventually be limited by single-core performance and disk I/O.
*   **Primary Limitation:** The entire system is constrained by the resources (CPU, RAM, Disk) of a **single machine**. This is the ultimate ceiling on scalability.

## 8. Future Evolution & Roadmap

This architecture provides a powerful foundation and a clear path for evolution as data volume and business needs grow.

### Phase 1: Optimization (Current Stack)

1.  **Refactor Click Attribution:** Rewrite the last-touch attribution to use a scalable window/ASOF-join pattern over sessionized impressions and events.
2.  **Partitioning:** For very large tables, write date-partitioned Parquet and push more work to efficient scans while keeping transactional integrity.
3.  **Optional Gold Materialization:** If required by BI tooling, add incremental materialization for key gold views using a watermark-based delete+insert per affected date.

### Phase 2: Migration to a Distributed Lakehouse

When single-node limits are reached, the next logical step is to migrate to a distributed, cloud-native architecture.

*   **Storage:** Move from a local DuckDB file to a data lake (e.g., AWS S3) with an open table format like **Apache Iceberg** or Delta Lake.
*   **Processing Engine:** Replace the Python/DuckDB scripts with **Apache Spark**. The core SQL logic and layered approach can be migrated directly to Spark SQL, which will distribute the workload across a cluster of machines.
*   **Orchestration:** Use a production-grade orchestrator like Airflow or Dagster to manage the Spark jobs.

### Phase 3: Real-time Streaming & ML Enablement

*   **Real-time Insights:** For use cases requiring millisecond latency (e.g., live dashboards, real-time personalization), a streaming layer can be introduced.
    *   **Architecture:** `AWS Kinesis/Kafka` (for event ingestion) -> `Apache Flink` (for stream processing and aggregation) -> `Druid/Pinot` (as a real-time OLAP database).
*   **Feature Store for Machine Learning:**
    *   The Gold Layer is the ideal source for an **offline feature store**. A new set of transformations can be built (using Spark) to generate user-level features (e.g., `purchases_last_30_days`, `recency`, `frequency`) for training models like conversion prediction or churn analysis.
    *   For real-time inference, these features would be served from a low-latency **online feature store** (e.g., Redis, DynamoDB), populated by the Flink streaming pipeline. This creates a unified, scalable MLOps platform.

