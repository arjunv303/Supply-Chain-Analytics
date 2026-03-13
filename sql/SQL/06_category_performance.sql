-- =============================================================================
-- 06_category_performance.sql
-- Supply Chain Analytics - Product Category Performance Analysis
--
-- Answers:
--   - Which categories drive the most revenue?
--   - Which categories have the best/worst fulfillment rates?
--   - How do review scores differ across categories?
--   - What price segments exist within each category?
-- =============================================================================

SET search_path TO olist;

-- =============================================================================
-- 1. TOP CATEGORIES BY REVENUE (Treemap / Bar Chart in Tableau)
-- =============================================================================

\echo '--- Top 20 Product Categories by Revenue ---'
SELECT
    COALESCE(pc.category_name_english, p.product_category_name, 'Uncategorized')
                                                                        AS category,
    COUNT(DISTINCT o.order_id)                                          AS total_orders,
    COUNT(DISTINCT oi.product_id)                                       AS unique_products,
    COUNT(DISTINCT oi.seller_id)                                        AS seller_count,

    ROUND(SUM(oi.price), 2)                                            AS total_product_revenue,
    ROUND(SUM(oi.freight_value), 2)                                    AS total_freight_revenue,
    ROUND(SUM(oi.price + oi.freight_value), 2)                         AS total_revenue,

    ROUND(AVG(oi.price), 2)                                            AS avg_item_price,
    ROUND(MIN(oi.price), 2)                                            AS min_price,
    ROUND(MAX(oi.price), 2)                                            AS max_price,

    ROUND(SUM(oi.price + oi.freight_value) * 100.0 /
        SUM(SUM(oi.price + oi.freight_value)) OVER (), 2)              AS pct_of_total_revenue

FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN products p ON oi.product_id = p.product_id
LEFT JOIN product_categories pc ON p.product_category_name = pc.category_name_portuguese
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 20;


-- =============================================================================
-- 2. CATEGORY FULFILLMENT PERFORMANCE
-- (Shows which categories are supply chain pain points)
-- =============================================================================

\echo ''
\echo '--- Category Fulfillment Performance ---'
SELECT
    COALESCE(pc.category_name_english, p.product_category_name, 'Uncategorized')
                                                                        AS category,
    COUNT(DISTINCT o.order_id)                                          AS total_orders,

    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400.0
    ) FILTER (WHERE o.order_delivered_customer_date IS NOT NULL), 1)    AS avg_lead_time_days,

    ROUND(
        COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date::DATE <= o.order_estimated_delivery_date
        ) * 100.0 /
        NULLIF(COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'delivered'), 0),
        2
    )                                                                   AS on_time_rate_pct,

    ROUND(AVG(r.review_score::NUMERIC), 2)                             AS avg_review_score,
    COUNT(r.review_id)                                                 AS review_count

FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN products p ON oi.product_id = p.product_id
LEFT JOIN product_categories pc ON p.product_category_name = pc.category_name_portuguese
LEFT JOIN order_reviews r ON o.order_id = r.order_id
WHERE o.order_status IN ('delivered', 'shipped', 'invoiced')
GROUP BY category
HAVING COUNT(DISTINCT o.order_id) >= 100
ORDER BY on_time_rate_pct ASC
LIMIT 25;


-- =============================================================================
-- 3. CATEGORY REVIEW SCORE DISTRIBUTION
-- (Stacked bar: % of 1-star, 2-star, ... 5-star reviews by category)
-- =============================================================================

\echo ''
\echo '--- Review Score Distribution by Category ---'
SELECT
    COALESCE(pc.category_name_english, p.product_category_name, 'Uncategorized')
                                                                        AS category,
    COUNT(r.review_id)                                                 AS total_reviews,
    ROUND(COUNT(r.review_id) FILTER (WHERE r.review_score = 1) * 100.0 / COUNT(r.review_id), 1) AS pct_1_star,
    ROUND(COUNT(r.review_id) FILTER (WHERE r.review_score = 2) * 100.0 / COUNT(r.review_id), 1) AS pct_2_star,
    ROUND(COUNT(r.review_id) FILTER (WHERE r.review_score = 3) * 100.0 / COUNT(r.review_id), 1) AS pct_3_star,
    ROUND(COUNT(r.review_id) FILTER (WHERE r.review_score = 4) * 100.0 / COUNT(r.review_id), 1) AS pct_4_star,
    ROUND(COUNT(r.review_id) FILTER (WHERE r.review_score = 5) * 100.0 / COUNT(r.review_id), 1) AS pct_5_star,
    ROUND(AVG(r.review_score::NUMERIC), 2)                             AS avg_score

FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN products p ON oi.product_id = p.product_id
LEFT JOIN product_categories pc ON p.product_category_name = pc.category_name_portuguese
JOIN order_reviews r ON o.order_id = r.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY category
HAVING COUNT(r.review_id) >= 50
ORDER BY avg_score DESC;


-- =============================================================================
-- 4. CATEGORY PRICE BAND SEGMENTATION
-- (Revenue by price tier per category — premium vs. budget product mix)
-- =============================================================================

\echo ''
\echo '--- Category Price Band Analysis ---'
SELECT
    COALESCE(pc.category_name_english, p.product_category_name, 'Uncategorized')
                                                                        AS category,
    CASE
        WHEN oi.price < 50   THEN 'Budget (<R$50)'
        WHEN oi.price < 200  THEN 'Mid-Range (R$50-200)'
        WHEN oi.price < 500  THEN 'Premium (R$200-500)'
        ELSE                      'Luxury (R$500+)'
    END                                                                 AS price_band,
    COUNT(*)                                                            AS item_count,
    ROUND(SUM(oi.price), 2)                                            AS revenue_in_band,
    ROUND(AVG(oi.price), 2)                                            AS avg_price

FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
LEFT JOIN product_categories pc ON p.product_category_name = pc.category_name_portuguese
GROUP BY category, price_band
ORDER BY category, MIN(oi.price);


-- =============================================================================
-- 5. MONTHLY CATEGORY REVENUE TREND (Top 5 Categories)
-- (Used for the AI Forecast dashboard — actual sales to compare against Prophet)
-- =============================================================================

\echo ''
\echo '--- Monthly Revenue by Top 5 Categories ---'
WITH top_categories AS (
    SELECT
        COALESCE(pc.category_name_english, p.product_category_name, 'Uncategorized') AS category,
        SUM(oi.price) AS total_revenue
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    LEFT JOIN product_categories pc ON p.product_category_name = pc.category_name_portuguese
    GROUP BY category
    ORDER BY total_revenue DESC
    LIMIT 5
)
SELECT
    COALESCE(pc.category_name_english, p.product_category_name, 'Uncategorized')
                                                                        AS category,
    DATE_TRUNC('month', o.order_purchase_timestamp)::DATE               AS order_month,
    COUNT(DISTINCT o.order_id)                                          AS monthly_orders,
    ROUND(SUM(oi.price), 2)                                            AS monthly_revenue

FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN products p ON oi.product_id = p.product_id
LEFT JOIN product_categories pc ON p.product_category_name = pc.category_name_portuguese
WHERE COALESCE(pc.category_name_english, p.product_category_name, 'Uncategorized') IN (
    SELECT category FROM top_categories
)
  AND o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY category, DATE_TRUNC('month', o.order_purchase_timestamp)
ORDER BY category, order_month;
