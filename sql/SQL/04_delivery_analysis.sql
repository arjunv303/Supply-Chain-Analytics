SET search_path TO olist;

-- Overall delivery performance
SELECT
    COUNT(*)                                                            AS total_delivered,
    COUNT(*) FILTER (
        WHERE order_delivered_customer_date::DATE <= order_estimated_delivery_date
    )                                                                   AS on_time,
    COUNT(*) FILTER (
        WHERE order_delivered_customer_date::DATE > order_estimated_delivery_date
    )                                                                   AS late,
    ROUND(
        COUNT(*) FILTER (
            WHERE order_delivered_customer_date::DATE > order_estimated_delivery_date
        ) * 100.0 / COUNT(*),
        2
    )                                                                   AS late_rate_pct,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            order_delivered_customer_date::DATE::TIMESTAMP -
            order_estimated_delivery_date::TIMESTAMP
        )) / 86400.0
    ) FILTER (
        WHERE order_delivered_customer_date::DATE > order_estimated_delivery_date
    ), 1)                                                               AS avg_days_late,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            order_estimated_delivery_date::TIMESTAMP -
            order_delivered_customer_date::DATE::TIMESTAMP
        )) / 86400.0
    ) FILTER (
        WHERE order_delivered_customer_date::DATE <= order_estimated_delivery_date
    ), 1)                                                               AS avg_days_early
FROM orders
WHERE order_status = 'delivered'
  AND order_delivered_customer_date IS NOT NULL;


-- Delivery delay by customer state
SELECT
    c.customer_state,
    COUNT(DISTINCT o.order_id)                                          AS total_orders,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400.0
    ), 1)                                                               AS avg_lead_time_days,
    ROUND(
        COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date::DATE > o.order_estimated_delivery_date
        ) * 100.0 /
        NULLIF(COUNT(DISTINCT o.order_id), 0),
        2
    )                                                                   AS late_rate_pct,
    ROUND(AVG(
        CASE
            WHEN o.order_delivered_customer_date::DATE > o.order_estimated_delivery_date
            THEN EXTRACT(EPOCH FROM (
                o.order_delivered_customer_date::DATE::TIMESTAMP -
                o.order_estimated_delivery_date::TIMESTAMP
            )) / 86400.0
            ELSE 0
        END
    ), 1)                                                               AS avg_days_late_for_late_orders
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY late_rate_pct DESC;


-- Delivery delay by product category
SELECT
    COALESCE(pc.category_name_english, p.product_category_name, 'Uncategorized')
                                                                        AS product_category,
    COUNT(DISTINCT o.order_id)                                          AS total_orders,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400.0
    ), 1)                                                               AS avg_lead_time_days,
    ROUND(
        COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date::DATE > o.order_estimated_delivery_date
        ) * 100.0 /
        NULLIF(COUNT(DISTINCT o.order_id), 0),
        2
    )                                                                   AS late_rate_pct,
    ROUND(AVG(p.product_weight_g), 0)                                  AS avg_product_weight_g
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
LEFT JOIN product_categories pc ON p.product_category_name = pc.category_name_portuguese
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
GROUP BY product_category
HAVING COUNT(DISTINCT o.order_id) >= 50
ORDER BY late_rate_pct DESC
LIMIT 25;


-- Seasonal delay patterns by month
SELECT
    TO_CHAR(o.order_purchase_timestamp, 'YYYY-MM')                      AS purchase_month,
    EXTRACT(MONTH FROM o.order_purchase_timestamp)                      AS month_num,
    TO_CHAR(o.order_purchase_timestamp, 'Month')                        AS month_name,
    COUNT(DISTINCT o.order_id)                                          AS orders_placed,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400.0
    ), 1)                                                               AS avg_lead_time_days,
    ROUND(
        COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date::DATE > o.order_estimated_delivery_date
        ) * 100.0 /
        NULLIF(COUNT(DISTINCT o.order_id), 0),
        2
    )                                                                   AS late_rate_pct
FROM orders o
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
GROUP BY purchase_month, month_num, month_name
ORDER BY purchase_month;


-- Freight weight vs delivery delay
SELECT
    CASE
        WHEN p.product_weight_g < 200   THEN 'Ultra Light (<200g)'
        WHEN p.product_weight_g < 1000  THEN 'Light (200g-1kg)'
        WHEN p.product_weight_g < 5000  THEN 'Medium (1kg-5kg)'
        WHEN p.product_weight_g < 15000 THEN 'Heavy (5kg-15kg)'
        ELSE                                 'Very Heavy (15kg+)'
    END                                                                 AS weight_bucket,
    COUNT(DISTINCT o.order_id)                                          AS order_count,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400.0
    ), 1)                                                               AS avg_lead_time_days,
    ROUND(
        COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date::DATE > o.order_estimated_delivery_date
        ) * 100.0 /
        NULLIF(COUNT(DISTINCT o.order_id), 0),
        2
    )                                                                   AS late_rate_pct,
    ROUND(AVG(oi.freight_value), 2)                                    AS avg_freight_charge
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
  AND p.product_weight_g IS NOT NULL
GROUP BY weight_bucket
ORDER BY MIN(p.product_weight_g);


-- Carrier handoff analysis
SELECT
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_carrier_delivery_date - o.order_purchase_timestamp
        )) / 86400.0
    ) FILTER (WHERE o.order_carrier_delivery_date IS NOT NULL), 1)      AS avg_seller_processing_days,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_carrier_delivery_date
        )) / 86400.0
    ) FILTER (
        WHERE o.order_carrier_delivery_date IS NOT NULL
          AND o.order_delivered_customer_date IS NOT NULL
    ), 1)                                                               AS avg_carrier_transit_days,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400.0
    ) FILTER (WHERE o.order_delivered_customer_date IS NOT NULL), 1)    AS avg_total_lead_time_days,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (o.order_carrier_delivery_date - o.order_purchase_timestamp)) / 86400.0)
        FILTER (WHERE o.order_carrier_delivery_date IS NOT NULL)
        * 100.0 /
        NULLIF(
            AVG(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)) / 86400.0)
            FILTER (WHERE o.order_delivered_customer_date IS NOT NULL),
        0),
        1
    )                                                                   AS pct_time_in_seller_hands
FROM orders o
WHERE o.order_status = 'delivered';
