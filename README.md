# gold_checkout_funnel_daily

Contains daily + platform aggregations of 
    - sessions
    - bounce rate
    - view_item_to_checkout conversion rate
    - checkout_to_purchase conversion rate
    - overall conversion rate

Questions this table can answer:
    - What are daily bounce rates by platform?
    - Where is drop-off highest: view_item → checkout or checkout → purchase?
    - How do funnel conversion rates trend over time by platform and overall?
    - Which days/platforms show unusual spikes or dips in conversion or bounce?

# gold_platform_daily_performance

Contains daily + platform aggregations of 
    - events
    - total revenue
    - avg order value
    - installment adoption rate

Questions this table can answer:
    - What is daily revenue by platform and how is it trending?
    - What is average order value by platform per day?
    - How is installment adoption rate changing by platform and day?
    - What share of revenue is contributed by each platform per day?

# mv_list_daily_performance

Contains daily + item_list aggregations of 
    - impressions
    - clicks
    - ctr

Questions this table can answer:
    - Which item lists have the highest CTR by day?
    - Which lists drive the most impressions and clicks?
    - How are list CTRs trending over time (improving vs declining)?
    - Which lists have high impressions but low CTR (optimization opportunities)?

# gold_user_sessions

Contains daily + user + session level aggregation of
    - events
    - had_view
    - had_checkout
    - had_purchase
Weakest so far.

Questions this table can answer:
    - What share of sessions included a view, checkout, or purchase?
    - What is the session-level bounce rate (sessions with a single event)?
    - What is the session view → purchase conversion rate?
    - How many sessions occur per day (and per user via counts)?

# gold_product_daily_performance

Contains daily + product level aggregations of
    - impressions
    - views
    - total revenue
    - conversion rate ???
    - avg_order_value ??? 

Questions this table can answer:
    - Which products receive the most impressions and views each day?
    - Which products generate the most daily revenue?
    - Which products convert well from views to purchases (requires purchase counts)?
    - Which products have high views but low revenue (potential drop-off)?

# vw_gold_product_campaign_price_change_events (Semantic View)

Shows price change events during active campaigns. Enabled by fct_product_campaigns table at (product_id, campaign_id, price, discount_value) grain.

Contains:
    - campaign_id, campaign_name, product_id
    - change_date, old_price, new_price, delta_pct

Business Questions:
    - Which products changed price during this campaign?
    - Did price increases happen during active campaigns (fake discount detection)?
    - Which campaigns had the most price volatility?

Growth Team Value:
    - Maintains campaign integrity by detecting fake discounts
    - Enables clean lift analysis by controlling for base price changes
    - Supports partner compliance monitoring

# vw_gold_product_campaign_current_status (Semantic View)

Shows current status of active campaign-product combinations. Enabled by fct_product_campaigns table.

Contains:
    - campaign_id, campaign_name, product_id, current_price
    - has_discount_today, has_discount_recent, current_discount_percentage (as percentage points)

Business Questions:
    - How many campaigns are currently running?
    - How many products are in discount right now?
    - What is the current discount percentage for each product?

Growth Team Value:
    - Real-time visibility into active campaign-product combinations
    - Quick identification of discounted products for audience targeting
    - Prevents discount stacking issues across multiple campaigns