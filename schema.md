## Warehouse Schemas (Markdown)

Generated from `setup_database.py`, `silver_transforms.py`, and `gold_transforms.py`.

### Legend
- PK: Primary key column (Yes/Composite)
- Nullable: whether NULLs are allowed
- Defaults: literal defaults or sequence-backed values

---

## Tables

### stg_events

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| event_id | VARCHAR |  | Yes |  |  |
| event_name | VARCHAR |  | Yes |  |  |
| event_timestamp | TIMESTAMP |  | Yes |  |  |
| replay_timestamp | TIMESTAMP |  | Yes |  |  |
| ingestion_timestamp | TIMESTAMP |  | Yes |  |  |
| user_id | VARCHAR |  | Yes |  |  |
| platform | VARCHAR |  | Yes |  | Raw platform code/name |
| item_list_id | VARCHAR |  | Yes |  |  |
| item_list_name | VARCHAR |  | Yes |  |  |
| product_id | VARCHAR |  | Yes |  |  |
| product_name | VARCHAR |  | Yes |  |  |
| price_in_usd | DECIMAL(12,6) |  | Yes |  |  |
| price | DECIMAL(12,2) |  | Yes |  | Local currency unless noted |
| quantity | INTEGER |  | Yes |  |  |
| item_revenue_in_usd | DECIMAL(12,6) |  | Yes |  |  |
| item_revenue | DECIMAL(12,2) |  | Yes |  |  |
| discount_value | DECIMAL(10,2) |  | Yes |  |  |
| total_price | DECIMAL(10,2) |  | Yes |  |  |
| discount_campaigns | VARCHAR |  | Yes |  | Pipe-delimited campaign codes |
| discount_experiment_variant | VARCHAR |  | Yes |  | Experiment/variant code |
| installments | INTEGER |  | Yes |  |  |
| installment_price | DECIMAL(10,2) |  | Yes |  |  |
| is_in_stock | BOOLEAN |  | Yes |  |  |
| raw_event | JSON |  | Yes |  | Original payload |
| processed_at | TIMESTAMP |  | Yes |  | Null until silver load marks processed |

Constraints & Indexes
- None (append-only; duplicates allowed)
- Indexes: `idx_stg_dedup_key(event_id, ingestion_timestamp)`, `idx_stg_processed(processed_at)`

---

### dim_dates

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| date_key | DATE | Yes | No |  |  |
| year | INTEGER |  | Yes |  |  |
| month | INTEGER |  | Yes |  |  |
| day | INTEGER |  | Yes |  |  |
| day_of_week | INTEGER |  | Yes |  | 0=Sunday |
| is_weekend | BOOLEAN |  | Yes |  |  |
| month_name | VARCHAR |  | Yes |  |  |
| quarter | INTEGER |  | Yes |  |  |

Constraints & Indexes
- PK: `(date_key)`

---

### dim_users

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| user_sk | BIGINT | Yes | No | NEXTVAL('user_sk_seq') | Surrogate key |
| user_id | VARCHAR |  | No |  | Natural key |
| source | VARCHAR |  | Yes |  |  |
| region | VARCHAR |  | Yes |  |  |
| customer_segment | VARCHAR |  | Yes |  |  |
| risk_segment | VARCHAR |  | Yes |  |  |
| credit_available | DECIMAL(12,2) |  | Yes |  |  |
| first_purchase_date | DATE |  | Yes |  |  |
| has_used_installments | BOOLEAN |  | Yes |  |  |
| effective_start_date | DATE |  | No |  | SCD2 start |
| effective_end_date | DATE |  | Yes |  | SCD2 end (NULL=open) |
| is_active | BOOLEAN |  | Yes | TRUE | SCD2 current flag |

Constraints & Indexes
- PK: `(user_sk)`
- UNIQUE: `(user_id, effective_start_date)`
- Indexes: `idx_users_nk(user_id)`, `idx_users_range(user_id, effective_start_date, effective_end_date)`

---

### dim_products

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| product_sk | BIGINT | Yes | No | NEXTVAL('product_sk_seq') | Surrogate key |
| product_id | VARCHAR |  | No |  | Natural key |
| product_name | VARCHAR |  | Yes |  |  |
| product_category | VARCHAR |  | Yes |  |  |
| price | DECIMAL(12,2) |  | Yes |  |  |
| effective_start_date | DATE |  | No |  | SCD2 start |
| effective_end_date | DATE |  | Yes |  | SCD2 end (NULL=open) |
| is_active | BOOLEAN |  | Yes | TRUE | SCD2 current flag |

Constraints & Indexes
- PK: `(product_sk)`
- UNIQUE: `(product_id, effective_start_date)`
- Indexes: `idx_products_nk(product_id)`, `idx_products_range(product_id, effective_start_date, effective_end_date)`

---

### dim_item_lists

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| item_list_sk | BIGINT | Yes | No | NEXTVAL('item_list_sk_seq') | Surrogate key |
| item_list_id | VARCHAR |  | No |  | Natural key |
| item_list_name | VARCHAR |  | Yes |  |  |
| list_type | VARCHAR |  | Yes |  | Upstream catalog |
| targeting_customer_segment | VARCHAR |  | Yes |  |  |
| targeting_region | VARCHAR |  | Yes |  |  |
| associated_experiment_variant | VARCHAR |  | Yes |  |  |
| planned_start_date | DATE |  | Yes |  |  |
| planned_end_date | DATE |  | Yes |  |  |
| effective_start_date | DATE |  | No |  | SCD2 start |
| effective_end_date | DATE |  | Yes |  | SCD2 end (NULL=open) |
| is_active | BOOLEAN |  | Yes | TRUE | SCD2 current flag |

Constraints & Indexes
- PK: `(item_list_sk)`
- UNIQUE: `(item_list_id, effective_start_date)`

---

### dim_platforms

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| platform_id | INTEGER | Yes | No |  |  |
| platform_name | VARCHAR |  | Yes |  | UNIQUE |

Constraints & Indexes
- PK: `(platform_id)`
- UNIQUE: `(platform_name)`
- Seed rows: `(1, 'IOS'), (2, 'ANDROID')`

---

### dim_campaigns

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| campaign_sk | BIGINT | Yes | No | NEXTVAL('campaign_sk_seq') | Surrogate key |
| campaign_id | VARCHAR |  | No |  | Natural key |
| campaign_name | VARCHAR |  | Yes |  |  |
| targeting_source | VARCHAR |  | Yes |  |  |
| targeting_segment | VARCHAR |  | Yes |  |  |
| targeting_risk_segment | VARCHAR |  | Yes |  |  |
| planned_start_date | DATE |  | Yes |  |  |
| planned_end_date | DATE |  | Yes |  |  |
| effective_start_date | DATE |  | No |  | SCD2 start |
| effective_end_date | DATE |  | Yes |  | SCD2 end (NULL=open) |
| is_active | BOOLEAN |  | Yes | TRUE | SCD2 current flag |

Constraints & Indexes
- PK: `(campaign_sk)`
- UNIQUE: `(campaign_id, effective_start_date)`

---

### dim_experiments

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| experiment_sk | BIGINT | Yes | No | NEXTVAL('experiment_sk_seq') | Surrogate key |
| experiment_id | VARCHAR |  | No |  | Natural key |
| experiment_name | VARCHAR |  | Yes |  |  |
| variant_name | VARCHAR |  | Yes |  |  |
| targeting_source | VARCHAR |  | Yes |  |  |
| targeting_segment | VARCHAR |  | Yes |  |  |
| targeting_risk_segment | VARCHAR |  | Yes |  |  |
| planned_start_date | DATE |  | Yes |  |  |
| planned_end_date | DATE |  | Yes |  |  |
| effective_start_date | DATE |  | No |  | SCD2 start |
| effective_end_date | DATE |  | Yes |  | SCD2 end (NULL=open) |
| is_active | BOOLEAN |  | Yes | TRUE | SCD2 current flag |

Constraints & Indexes
- PK: `(experiment_sk)`
- UNIQUE: `(experiment_id, effective_start_date)`

---

### fct_events

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| event_id | VARCHAR | Yes | No |  | Event primary key |
| event_timestamp | TIMESTAMP |  | Yes |  |  |
| event_name | VARCHAR |  | Yes |  |  |
| user_id | VARCHAR |  | Yes |  |  |
| product_id | VARCHAR |  | Yes |  |  |
| item_list_id | VARCHAR |  | Yes |  |  |
| platform_id | INTEGER |  | Yes |  | FK to dim_platforms |
| date_id | DATE |  | Yes |  |  |
| campaign_codes | VARCHAR |  | Yes |  | Degenerate dimension |
| experiment_id | VARCHAR |  | Yes |  | Degenerate dimension |
| price_in_usd | DECIMAL(12,6) |  | Yes |  |  |
| price | DECIMAL(12,2) |  | Yes |  |  |
| quantity | INTEGER |  | Yes |  |  |
| item_revenue_in_usd | DECIMAL(12,6) |  | Yes |  |  |
| item_revenue | DECIMAL(12,2) |  | Yes |  |  |
| discount_value | DECIMAL(10,2) |  | Yes |  |  |
| total_price | DECIMAL(10,2) |  | Yes |  |  |
| installments | INTEGER |  | Yes |  |  |
| installment_price | DECIMAL(10,2) |  | Yes |  |  |
| is_in_stock | BOOLEAN |  | Yes |  |  |
| ingestion_timestamp | TIMESTAMP |  | Yes |  |  |

Constraints & Indexes
- PK: `(event_id)`
- Indexes: `idx_fct_user_ts(user_id, event_timestamp)`, `idx_fct_date(date_id)`

---

### silver_event_list_attribution

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| event_id | VARCHAR | Yes | No |  |  |
| user_id | VARCHAR |  | Yes |  |  |
| event_name | VARCHAR |  | Yes |  |  |
| event_timestamp | TIMESTAMP |  | Yes |  |  |
| item_list_id | VARCHAR |  | Yes |  |  |
| platform_id | INTEGER |  | Yes |  |  |
| session_id | VARCHAR |  | Yes |  |  |
| impression_ts_chosen | TIMESTAMP |  | Yes |  |  |
| attribution_model | VARCHAR |  | Yes |  | e.g., 'last_touch_session' |
| window_minutes | INTEGER |  | Yes |  |  |
| attribution_version | INTEGER |  | Yes |  |  |
| date_id | DATE |  | Yes |  |  |

Constraints & Indexes
- PK: `(event_id)`

---

### silver_list_impressions

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| user_id | VARCHAR | Yes (composite) | No |  |  |
| platform_id | INTEGER | Yes (composite) | No |  |  |
| session_id | VARCHAR | Yes (composite) | No |  |  |
| session_start_ts | TIMESTAMP | Yes (composite) | No |  |  |
| session_date | DATE |  | Yes |  |  |
| item_list_id | VARCHAR | Yes (composite) | No |  |  |
| impression_ts | TIMESTAMP | Yes (composite) | No |  |  |
| date_id | DATE |  | Yes |  |  |

Constraints & Indexes
- PK (composite): `(user_id, platform_id, session_start_ts, item_list_id, impression_ts)`

---

### silver_user_sessions

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| user_id | VARCHAR | Yes (composite) | No |  |  |
| platform_id | INTEGER | Yes (composite) | No |  |  |
| session_start_ts | TIMESTAMP | Yes (composite) | No |  |  |
| session_end_ts | TIMESTAMP |  | Yes |  |  |
| session_date | DATE |  | Yes |  |  |
| events_in_session | INTEGER |  | Yes |  |  |
| views | INTEGER |  | Yes |  |  |
| checkouts | INTEGER |  | Yes |  |  |
| purchases | INTEGER |  | Yes |  |  |

Constraints & Indexes
- PK (composite): `(user_id, platform_id, session_start_ts)`

---

### silver_session_features

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| user_id | VARCHAR | Yes (composite) | No |  |  |
| platform_id | INTEGER | Yes (composite) | No |  |  |
| session_start_ts | TIMESTAMP | Yes (composite) | No |  |  |
| session_end_ts | TIMESTAMP |  | Yes |  |  |
| session_date | DATE |  | Yes |  |  |
| events_in_session | INTEGER |  | Yes |  |  |
| had_view | BOOLEAN |  | Yes |  |  |
| had_checkout | BOOLEAN |  | Yes |  |  |
| had_purchase | BOOLEAN |  | Yes |  |  |
| is_bounce | BOOLEAN |  | Yes |  |  |
| is_abandoned_checkout | BOOLEAN |  | Yes |  |  |
| cart_value_usd | DECIMAL(12,6) |  | Yes |  |  |
| cart_products | VARCHAR[] |  | Yes |  | Array of product_ids |
| is_high_value_abandoned_checkout | BOOLEAN |  | Yes |  |  |

Constraints & Indexes
- PK (composite): `(user_id, platform_id, session_start_ts)`

---

### fct_product_campaigns

| Column | Type | PK | Nullable | Default | Notes |
|---|---|---|---|---|---|
| product_id | VARCHAR | Yes (composite) | No |  |  |
| campaign_id | VARCHAR | Yes (composite) | No |  |  |
| price | DECIMAL(12,2) | Yes (composite) | No |  |  |
| discount_value | DECIMAL(10,2) | Yes (composite) | No |  |  |
| total_events | INTEGER |  | No |  | Aggregated count |
| views | INTEGER |  | No |  |  |
| checkouts | INTEGER |  | No |  |  |
| purchases | INTEGER |  | No |  |  |
| revenue | DECIMAL(12,2) |  | No |  |  |
| first_seen_date | DATE |  | No |  |  |
| last_seen_date | DATE |  | No |  |  |

Constraints & Indexes
- PK (composite): `(product_id, campaign_id, price, discount_value)`
- Indexes: `idx_fpc_campaign(campaign_id)`, `idx_fpc_product(product_id)`

---

## Views

### silver_purchases (view)

| Column | Type | Notes |
|---|---|---|
| event_id | VARCHAR | From `fct_events` |
| user_id | VARCHAR |  |
| platform_id | INTEGER |  |
| product_id | VARCHAR |  |
| item_list_id | VARCHAR |  |
| item_revenue | DECIMAL(12,2) |  |
| installments | INTEGER |  |
| installment_price | DECIMAL(10,2) |  |
| total_price | DECIMAL(10,2) |  |
| price | DECIMAL(12,2) |  |
| discount_value | DECIMAL(10,2) |  |
| quantity | INTEGER |  |
| event_timestamp | TIMESTAMP |  |
| event_date | DATE | Derived: `DATE(event_timestamp)` |

---

### vw_gold_product_campaign_price_change_events (view)

| Column | Type | Notes |
|---|---|---|
| campaign_id | VARCHAR |  |
| campaign_name | VARCHAR |  |
| product_id | VARCHAR |  |
| change_date | DATE | From `dim_products.effective_start_date` |
| old_price | DECIMAL(12,2) |  |
| new_price | DECIMAL(12,2) |  |
| delta_pct | DECIMAL | `(new_price - old_price) / NULLIF(old_price, 0)` |

---

### vw_gold_product_campaign_current_status (view)

| Column | Type | Notes |
|---|---|---|
| campaign_id | VARCHAR | Active campaigns only |
| campaign_name | VARCHAR |  |
| product_id | VARCHAR | Active products only |
| current_price | DECIMAL(12,2) | From `dim_products` |
| has_discount_today | INTEGER | 0/1 flag based on `last_seen_date = CURRENT_DATE` |
| has_discount_recent | INTEGER | 0/1 flag if any discount observed |
| current_discount_percentage | DECIMAL | Max discount_value / price (%) for today |

---

### vw_growth_engagement_daily (view)

| Column | Type | Notes |
|---|---|---|
| date | DATE | Canonical session date |
| platform_id | INTEGER |  |
| campaign_id | VARCHAR | From last-touch; 'NONE' if missing |
| item_list_id | VARCHAR |  |
| sessions | INTEGER | Distinct sessions |
| sessions_exposed | INTEGER | Sessions with list exposure |
| list_impressions | INTEGER | Sum of impressions |
| sessions_with_click | INTEGER | Sessions containing at least one `view_item` |
| sessions_with_checkout | INTEGER | Sessions containing at least one `begin_checkout` |
| sessions_with_purchase | INTEGER | Sessions containing at least one `purchase` |
| ctr_sessions | DECIMAL | had_click / sessions_exposed |
| purchase_rate_on_exposure | DECIMAL | had_purchase / sessions_exposed |
| purchase_rate_on_click | DECIMAL | had_purchase / had_click |

---

### gold_segment_abandon_then_bought_different_v (view)

| Column | Type | Notes |
|---|---|---|
| user_id | VARCHAR |  |
| platform_id | INTEGER |  |
| session_start_ts | TIMESTAMP | From abandoned checkout session |

---

### gold_segment_bounce_v (view)

Columns identical to `silver_session_features`; filtered where `is_bounce = TRUE`.

| Column | Type |
|---|---|
| user_id | VARCHAR |
| platform_id | INTEGER |
| session_start_ts | TIMESTAMP |
| session_end_ts | TIMESTAMP |
| session_date | DATE |
| events_in_session | INTEGER |
| had_view | BOOLEAN |
| had_checkout | BOOLEAN |
| had_purchase | BOOLEAN |
| is_bounce | BOOLEAN |
| is_abandoned_checkout | BOOLEAN |
| cart_value_usd | DECIMAL(12,6) |
| cart_products | VARCHAR[] |
| is_high_value_abandoned_checkout | BOOLEAN |

---

### gold_segment_checkout_abandon_v (view)

Columns identical to `silver_session_features`; filtered where `is_abandoned_checkout = TRUE`.

| Column | Type |
|---|---|
| user_id | VARCHAR |
| platform_id | INTEGER |
| session_start_ts | TIMESTAMP |
| session_end_ts | TIMESTAMP |
| session_date | DATE |
| events_in_session | INTEGER |
| had_view | BOOLEAN |
| had_checkout | BOOLEAN |
| had_purchase | BOOLEAN |
| is_bounce | BOOLEAN |
| is_abandoned_checkout | BOOLEAN |
| cart_value_usd | DECIMAL(12,6) |
| cart_products | VARCHAR[] |
| is_high_value_abandoned_checkout | BOOLEAN |

---

### gold_segment_high_value_abandon_v (view)

Columns identical to `silver_session_features`; filtered where `is_high_value_abandoned_checkout = TRUE`.

| Column | Type |
|---|---|
| user_id | VARCHAR |
| platform_id | INTEGER |
| session_start_ts | TIMESTAMP |
| session_end_ts | TIMESTAMP |
| session_date | DATE |
| events_in_session | INTEGER |
| had_view | BOOLEAN |
| had_checkout | BOOLEAN |
| had_purchase | BOOLEAN |
| is_bounce | BOOLEAN |
| is_abandoned_checkout | BOOLEAN |
| cart_value_usd | DECIMAL(12,6) |
| cart_products | VARCHAR[] |
| is_high_value_abandoned_checkout | BOOLEAN |


