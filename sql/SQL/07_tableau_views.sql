-- =============================================================================
-- 07_tableau_views.sql
-- Supply Chain Analytics - Views for Tableau Export
-- =============================================================================

SET search_path TO olist;

-- =============================================================================
-- VIEW 1: EXECUTIVE KPI DASHBOARD
-- =============================================================================

DROP VIEW IF EXISTS vw_exec_kpis;
CREATE VIEW vw_exec_kpis AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp)::DATE               AS order_month,
    COUNT(DISTINCT o.order_id)                                          AS total_orders,
    COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'delivered') AS delivered_orders,
    COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'canceled')  AS canceled_orders,

    ROUND(
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'delivered')
        * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0), 2
    )                                                                   AS fulfillment_rate_pct,

    ROUND(
        COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date IS NOT NULL
              AND o.order_delivered_customer_date::DATE <= o.order_estimated_delivery_date::DATE
        ) * 100.0 /
        NULLIF(COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'delivered'), 0),
        2
    )                                                                   AS on_time_delivery_rate_pct,

    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400.0
    ) FILTER (WHERE o.order_delivered_customer_date IS NOT NULL)::NUMERIC, 1) AS avg_lead_time_days,

    ROUND(SUM(pay.payment_value)::NUMERIC, 2)                          AS total_revenue,
    ROUND(AVG(pay.payment_value)::NUMERIC, 2)                          AS avg_order_value,
    ROUND(AVG(r.review_score::NUMERIC), 2)                             AS avg_review_score,
    COUNT(DISTINCT o.customer_id)                                       AS unique_customers

FROM orders o
JOIN (
    SELECT order_id, SUM(payment_value) AS payment_value
    FROM order_payments GROUP BY order_id
) pay ON o.order_id = pay.order_id
LEFT JOIN order_reviews r ON o.order_id = r.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp)
ORDER BY order_month;


-- =============================================================================
-- VIEW 2: SELLER SCORECARD DASHBOARD
-- =============================================================================

DROP VIEW IF EXISTS vw_seller_scorecard;
CREATE VIEW vw_seller_scorecard AS
WITH seller_metrics AS (
    SELECT
        s.seller_id, s.seller_city, s.seller_state,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        ROUND(SUM(oi.price + oi.freight_value)::NUMERIC, 2) AS total_revenue,
        ROUND(AVG(oi.price)::NUMERIC, 2) AS avg_item_price,
        ROUND(COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date IS NOT NULL
              AND o.order_delivered_customer_date::DATE <= o.order_estimated_delivery_date::DATE
        ) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'delivered'), 0), 2) AS on_time_rate_pct,
        ROUND(AVG(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)) / 86400.0)
              FILTER (WHERE o.order_delivered_customer_date IS NOT NULL)::NUMERIC, 1) AS avg_lead_time_days,
        ROUND(AVG(r.review_score::NUMERIC), 2) AS avg_review_score,
        COUNT(r.review_id) AS total_reviews
    FROM sellers s
    JOIN order_items oi ON s.seller_id = oi.seller_id
    JOIN orders o ON oi.order_id = o.order_id
    LEFT JOIN order_reviews r ON o.order_id = r.order_id
    GROUP BY s.seller_id, s.seller_city, s.seller_state
    HAVING COUNT(DISTINCT oi.order_id) >= 5
),
scored AS (
    SELECT *,
        ROUND((
            (COALESCE(on_time_rate_pct, 0) * 0.40) +
            (((COALESCE(avg_review_score, 0) - 1) * 25.0) * 0.30) +
            ((total_revenue - MIN(total_revenue) OVER ()) * 100.0 /
             NULLIF(MAX(total_revenue) OVER () - MIN(total_revenue) OVER (), 0) * 0.30)
        )::NUMERIC, 2) AS composite_score,
        CASE WHEN total_orders >= 200 THEN 'High Volume'
             WHEN total_orders >= 50  THEN 'Mid Volume'
             ELSE 'Low Volume' END AS volume_tier
    FROM seller_metrics
)
SELECT *, RANK() OVER (ORDER BY composite_score DESC) AS performance_rank
FROM scored
ORDER BY composite_score DESC;


-- =============================================================================
-- VIEW 3: REGIONAL DEMAND DASHBOARD
-- =============================================================================

DROP VIEW IF EXISTS vw_regional_demand;
CREATE VIEW vw_regional_demand AS
SELECT
    c.customer_state                                                    AS state,
    CASE c.customer_state
        WHEN 'SP' THEN 'Southeast' WHEN 'RJ' THEN 'Southeast'
        WHEN 'MG' THEN 'Southeast' WHEN 'ES' THEN 'Southeast'
        WHEN 'RS' THEN 'South'    WHEN 'SC' THEN 'South'
        WHEN 'PR' THEN 'South'
        WHEN 'BA' THEN 'Northeast' WHEN 'PE' THEN 'Northeast'
        WHEN 'CE' THEN 'Northeast' WHEN 'MA' THEN 'Northeast'
        WHEN 'PB' THEN 'Northeast' WHEN 'RN' THEN 'Northeast'
        WHEN 'AL' THEN 'Northeast' WHEN 'SE' THEN 'Northeast'
        WHEN 'PI' THEN 'Northeast'
        WHEN 'PA' THEN 'North'    WHEN 'AM' THEN 'North'
        WHEN 'RO' THEN 'North'    WHEN 'AC' THEN 'North'
        WHEN 'AP' THEN 'North'    WHEN 'RR' THEN 'North'
        WHEN 'TO' THEN 'North'
        WHEN 'DF' THEN 'Central-West' WHEN 'GO' THEN 'Central-West'
        WHEN 'MT' THEN 'Central-West' WHEN 'MS' THEN 'Central-West'
        ELSE 'Other'
    END                                                                 AS region,
    COUNT(DISTINCT o.order_id)                                          AS total_orders,
    COUNT(DISTINCT c.customer_unique_id)                                AS unique_customers,
    ROUND(SUM(pay.payment_value)::NUMERIC, 2)                          AS total_revenue,
    ROUND(AVG(pay.payment_value)::NUMERIC, 2)                          AS avg_order_value,
    ROUND(
        COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date IS NOT NULL
              AND o.order_delivered_customer_date::DATE > o.order_estimated_delivery_date::DATE
        ) * 100.0 /
        NULLIF(COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'delivered'), 0),
        2
    )                                                                   AS late_delivery_rate_pct,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400.0
    ) FILTER (WHERE o.order_delivered_customer_date IS NOT NULL)::NUMERIC, 1) AS avg_lead_time_days,
    ROUND(AVG(r.review_score::NUMERIC), 2)                             AS avg_review_score
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN (
    SELECT order_id, SUM(payment_value) AS payment_value
    FROM order_payments GROUP BY order_id
) pay ON o.order_id = pay.order_id
LEFT JOIN order_reviews r ON o.order_id = r.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.customer_state, region
ORDER BY total_revenue DESC;


-- =============================================================================
-- VIEW 4: CATEGORY DAILY ORDERS (for AI Forecast)
-- =============================================================================

DROP VIEW IF EXISTS vw_category_daily_orders;
CREATE VIEW vw_category_daily_orders AS
SELECT
    DATE_TRUNC('day', o.order_purchase_timestamp)::DATE                 AS order_date,
    COALESCE(pc.product_category_name_english, p.product_category_name, 'Uncategorized')
                                                                        AS category,
    COUNT(DISTINCT o.order_id)                                          AS daily_orders,
    ROUND(SUM(oi.price)::NUMERIC, 2)                                   AS daily_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
LEFT JOIN product_categories pc ON p.product_category_name = pc.product_category_name
WHERE o.order_status NOT IN ('canceled', 'unavailable')
  AND o.order_purchase_timestamp IS NOT NULL
GROUP BY DATE_TRUNC('day', o.order_purchase_timestamp), category
ORDER BY order_date, category;


-- =============================================================================
-- VERIFY
-- =============================================================================

\echo ''
\echo '=== View Row Counts ==='
SELECT 'vw_exec_kpis'             AS view_name, COUNT(*) AS rows FROM vw_exec_kpis
UNION ALL
SELECT 'vw_seller_scorecard',      COUNT(*) FROM vw_seller_scorecard
UNION ALL
SELECT 'vw_regional_demand',       COUNT(*) FROM vw_regional_demand
UNION ALL
SELECT 'vw_category_daily_orders', COUNT(*) FROM vw_category_daily_orders;

\echo 'Tableau views created successfully.'
