-- =============================================================================
-- 01_data_quality.sql
-- Supply Chain Analytics - Data Quality Validation
-- Run after loading data to identify issues before analysis
-- =============================================================================

SET search_path TO olist;

\echo '============================================================'
\echo 'DATA QUALITY REPORT - Olist E-Commerce Supply Chain Dataset'
\echo '============================================================'

-- =============================================================================
-- 1. NULL / MISSING VALUE ANALYSIS
-- =============================================================================

\echo ''
\echo '--- 1. NULL Counts in Orders Table (Critical Fields) ---'
SELECT
    COUNT(*)                                                            AS total_orders,
    COUNT(*) FILTER (WHERE order_purchase_timestamp IS NULL)            AS null_purchase_ts,
    COUNT(*) FILTER (WHERE order_approved_at IS NULL)                   AS null_approved_at,
    COUNT(*) FILTER (WHERE order_carrier_delivery_date IS NULL)         AS null_carrier_date,
    COUNT(*) FILTER (WHERE order_delivered_customer_date IS NULL)       AS null_delivered_date,
    COUNT(*) FILTER (WHERE order_estimated_delivery_date IS NULL)       AS null_estimated_date,
    ROUND(
        COUNT(*) FILTER (WHERE order_delivered_customer_date IS NULL)
        * 100.0 / COUNT(*), 2
    )                                                                   AS pct_undelivered
FROM orders;

\echo ''
\echo '--- 2. NULL Counts in Products Table ---'
SELECT
    COUNT(*)                                                            AS total_products,
    COUNT(*) FILTER (WHERE product_category_name IS NULL)               AS null_category,
    COUNT(*) FILTER (WHERE product_weight_g IS NULL)                    AS null_weight,
    COUNT(*) FILTER (WHERE product_length_cm IS NULL)                   AS null_dimensions
FROM products;

-- =============================================================================
-- 2. DUPLICATE DETECTION
-- =============================================================================

\echo ''
\echo '--- 3. Duplicate Order IDs ---'
SELECT
    order_id,
    COUNT(*) AS occurrences
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 10;

\echo ''
\echo '--- 4. Duplicate Review IDs per Order ---'
SELECT
    order_id,
    COUNT(*) AS review_count
FROM order_reviews
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY review_count DESC
LIMIT 10;

-- =============================================================================
-- 3. DATE RANGE & INTEGRITY CHECKS
-- =============================================================================

\echo ''
\echo '--- 5. Date Range of Orders Dataset ---'
SELECT
    MIN(order_purchase_timestamp)::DATE     AS earliest_order,
    MAX(order_purchase_timestamp)::DATE     AS latest_order,
    COUNT(DISTINCT DATE_TRUNC('month', order_purchase_timestamp)) AS months_covered
FROM orders;

\echo ''
\echo '--- 6. Orders Where Actual Delivery BEFORE Purchase (Data Anomaly) ---'
SELECT COUNT(*) AS impossible_deliveries
FROM orders
WHERE order_delivered_customer_date < order_purchase_timestamp;

\echo ''
\echo '--- 7. Orders Where Actual Delivery BEFORE Carrier Pickup (Data Anomaly) ---'
SELECT COUNT(*) AS impossible_carrier_timing
FROM orders
WHERE order_delivered_customer_date < order_carrier_delivery_date;

-- =============================================================================
-- 4. REFERENTIAL INTEGRITY CHECKS
-- =============================================================================

\echo ''
\echo '--- 8. Order Items With No Matching Order (Orphaned Records) ---'
SELECT COUNT(*) AS orphaned_items
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

\echo ''
\echo '--- 9. Order Items With No Matching Product ---'
SELECT COUNT(*) AS items_missing_product
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

\echo ''
\echo '--- 10. Order Items With No Matching Seller ---'
SELECT COUNT(*) AS items_missing_seller
FROM order_items oi
LEFT JOIN sellers s ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL;

-- =============================================================================
-- 5. ORDER STATUS DISTRIBUTION
-- =============================================================================

\echo ''
\echo '--- 11. Order Status Distribution ---'
SELECT
    order_status,
    COUNT(*)                                        AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM orders
GROUP BY order_status
ORDER BY order_count DESC;

-- =============================================================================
-- 6. PRICE OUTLIER DETECTION
-- =============================================================================

\echo ''
\echo '--- 12. Price Outliers in Order Items (Beyond 3 Std Dev) ---'
WITH stats AS (
    SELECT
        AVG(price)    AS avg_price,
        STDDEV(price) AS std_price
    FROM order_items
    WHERE price > 0
)
SELECT
    COUNT(*) AS outlier_items,
    MIN(price) AS min_outlier_price,
    MAX(price) AS max_outlier_price
FROM order_items, stats
WHERE price > avg_price + (3 * std_price)
   OR price < avg_price - (3 * std_price);

\echo ''
\echo '--- 13. Zero or Negative Prices ---'
SELECT
    COUNT(*) FILTER (WHERE price = 0)   AS zero_price_items,
    COUNT(*) FILTER (WHERE price < 0)   AS negative_price_items,
    COUNT(*) FILTER (WHERE freight_value < 0) AS negative_freight_items
FROM order_items;

\echo ''
\echo '============================================================'
\echo 'Data Quality Report Complete.'
\echo '============================================================'
