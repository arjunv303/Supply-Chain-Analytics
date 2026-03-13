-- =============================================================================
-- 02_order_fulfillment.sql
-- Supply Chain Analytics - Order Fulfillment KPIs
--
-- Key Metrics:
--   - On-time delivery rate (actual vs estimated delivery date)
--   - Average fulfillment lead time (purchase → delivered)
--   - Monthly fulfillment performance trend
--   - Order status funnel
--   - Approval lag (purchase → approved)
-- =============================================================================

SET search_path TO olist;

-- =============================================================================
-- 1. OVERALL FULFILLMENT KPIs (Headline Numbers for Executive Dashboard)
-- =============================================================================

\echo '--- Overall Fulfillment KPIs ---'
SELECT
    COUNT(*)                                                            AS total_orders,

    -- On-time delivery: delivered on or before estimated date
    COUNT(*) FILTER (
        WHERE order_delivered_customer_date IS NOT NULL
          AND order_delivered_customer_date::DATE <= order_estimated_delivery_date
    )                                                                   AS delivered_on_time,

    ROUND(
        COUNT(*) FILTER (
            WHERE order_delivered_customer_date IS NOT NULL
              AND order_delivered_customer_date::DATE <= order_estimated_delivery_date
        ) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE order_delivered_customer_date IS NOT NULL), 0),
        2
    )                                                                   AS on_time_delivery_rate_pct,

    -- Average lead time: purchase timestamp to customer delivery
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            order_delivered_customer_date - order_purchase_timestamp
        )) / 86400.0
    ) FILTER (WHERE order_delivered_customer_date IS NOT NULL), 1)      AS avg_lead_time_days,

    -- Average approval lag: purchase to payment approval
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            order_approved_at - order_purchase_timestamp
        )) / 3600.0
    ) FILTER (WHERE order_approved_at IS NOT NULL), 1)                  AS avg_approval_lag_hours,

    -- Average days early or late vs. estimate
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            order_estimated_delivery_date::TIMESTAMP - order_delivered_customer_date
        )) / 86400.0
    ) FILTER (WHERE order_delivered_customer_date IS NOT NULL), 1)      AS avg_days_early_positive_late_negative

FROM orders
WHERE order_status = 'delivered';


-- =============================================================================
-- 2. MONTHLY FULFILLMENT TREND
-- (Used for the time-series line chart in Tableau Executive Dashboard)
-- =============================================================================

\echo ''
\echo '--- Monthly Fulfillment Trend ---'
SELECT
    DATE_TRUNC('month', order_purchase_timestamp)::DATE                 AS order_month,

    COUNT(*)                                                            AS total_orders,

    COUNT(*) FILTER (WHERE order_status = 'delivered')                  AS delivered_orders,

    COUNT(*) FILTER (
        WHERE order_delivered_customer_date IS NOT NULL
          AND order_delivered_customer_date::DATE <= order_estimated_delivery_date
    )                                                                   AS on_time_orders,

    ROUND(
        COUNT(*) FILTER (
            WHERE order_delivered_customer_date IS NOT NULL
              AND order_delivered_customer_date::DATE <= order_estimated_delivery_date
        ) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE order_status = 'delivered'), 0),
        2
    )                                                                   AS on_time_rate_pct,

    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            order_delivered_customer_date - order_purchase_timestamp
        )) / 86400.0
    ) FILTER (WHERE order_delivered_customer_date IS NOT NULL), 1)      AS avg_lead_time_days,

    -- Month-over-month order volume change
    COUNT(*) - LAG(COUNT(*)) OVER (
        ORDER BY DATE_TRUNC('month', order_purchase_timestamp)
    )                                                                   AS mom_order_volume_change

FROM orders
GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
ORDER BY order_month;


-- =============================================================================
-- 3. ORDER STATUS FUNNEL
-- (Waterfall / funnel chart in Tableau Executive Dashboard)
-- =============================================================================

\echo ''
\echo '--- Order Status Funnel ---'
SELECT
    order_status,
    COUNT(*)                                                            AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)                 AS pct_of_all_orders,

    -- Average days spent in each status
    ROUND(AVG(
        CASE order_status
            WHEN 'approved'   THEN EXTRACT(EPOCH FROM (order_approved_at - order_purchase_timestamp)) / 86400.0
            WHEN 'delivered'  THEN EXTRACT(EPOCH FROM (order_delivered_customer_date - order_approved_at)) / 86400.0
            ELSE NULL
        END
    ), 1)                                                               AS avg_days_in_status

FROM orders
GROUP BY order_status
ORDER BY
    CASE order_status
        WHEN 'created'              THEN 1
        WHEN 'approved'             THEN 2
        WHEN 'processing'           THEN 3
        WHEN 'invoiced'             THEN 4
        WHEN 'shipped'              THEN 5
        WHEN 'delivered'            THEN 6
        WHEN 'canceled'             THEN 7
        WHEN 'unavailable'          THEN 8
        ELSE 9
    END;


-- =============================================================================
-- 4. LEAD TIME DISTRIBUTION (Histogram Buckets)
-- (Bar chart showing lead time spread in Tableau)
-- =============================================================================

\echo ''
\echo '--- Lead Time Distribution (Days) ---'
SELECT
    CASE
        WHEN lead_time_days <= 3  THEN '0-3 days'
        WHEN lead_time_days <= 7  THEN '4-7 days'
        WHEN lead_time_days <= 14 THEN '8-14 days'
        WHEN lead_time_days <= 21 THEN '15-21 days'
        WHEN lead_time_days <= 30 THEN '22-30 days'
        ELSE '30+ days'
    END                                                                 AS lead_time_bucket,

    COUNT(*)                                                            AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)                 AS pct_of_delivered

FROM (
    SELECT
        EXTRACT(EPOCH FROM (
            order_delivered_customer_date - order_purchase_timestamp
        )) / 86400.0                                                    AS lead_time_days
    FROM orders
    WHERE order_delivered_customer_date IS NOT NULL
      AND order_status = 'delivered'
) t
GROUP BY lead_time_bucket
ORDER BY MIN(lead_time_days);


-- =============================================================================
-- 5. WEEKEND vs. WEEKDAY ORDER PATTERNS
-- (Insight into fulfillment speed based on day of purchase)
-- =============================================================================

\echo ''
\echo '--- Weekend vs Weekday Fulfillment Performance ---'
SELECT
    CASE
        WHEN EXTRACT(DOW FROM order_purchase_timestamp) IN (0, 6)
        THEN 'Weekend'
        ELSE 'Weekday'
    END                                                                 AS day_type,

    COUNT(*)                                                            AS order_count,

    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            order_delivered_customer_date - order_purchase_timestamp
        )) / 86400.0
    ) FILTER (WHERE order_delivered_customer_date IS NOT NULL), 1)      AS avg_lead_time_days,

    ROUND(
        COUNT(*) FILTER (
            WHERE order_delivered_customer_date IS NOT NULL
              AND order_delivered_customer_date::DATE <= order_estimated_delivery_date
        ) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE order_status = 'delivered'), 0),
        2
    )                                                                   AS on_time_rate_pct

FROM orders
GROUP BY day_type
ORDER BY day_type;
