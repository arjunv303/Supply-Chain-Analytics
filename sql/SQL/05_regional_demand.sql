SET search_path TO olist;

-- Revenue and order volume by state
SELECT
    c.customer_state                                                    AS state,
    COUNT(DISTINCT o.order_id)                                          AS total_orders,
    COUNT(DISTINCT c.customer_unique_id)                                AS unique_customers,
    ROUND(SUM(pay.payment_value), 2)                                   AS total_revenue,
    ROUND(AVG(pay.payment_value), 2)                                   AS avg_order_value,
    ROUND(MEDIAN(pay.payment_value::NUMERIC), 2)                       AS median_order_value,
    ROUND(SUM(pay.payment_value) * 100.0 / SUM(SUM(pay.payment_value)) OVER (), 2)
                                                                        AS pct_of_total_revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN (
    SELECT order_id, SUM(payment_value) AS payment_value
    FROM order_payments GROUP BY order_id
) pay ON o.order_id = pay.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.customer_state
ORDER BY total_revenue DESC;


-- Monthly order volume by state (for growth trend analysis)
SELECT
    c.customer_state                                                    AS state,
    DATE_TRUNC('month', o.order_purchase_timestamp)::DATE               AS order_month,
    COUNT(DISTINCT o.order_id)                                          AS monthly_orders,
    ROUND(SUM(pay.payment_value), 2)                                   AS monthly_revenue,
    ROUND(
        (COUNT(DISTINCT o.order_id) - LAG(COUNT(DISTINCT o.order_id)) OVER (
            PARTITION BY c.customer_state
            ORDER BY DATE_TRUNC('month', o.order_purchase_timestamp)
        )) * 100.0 /
        NULLIF(LAG(COUNT(DISTINCT o.order_id)) OVER (
            PARTITION BY c.customer_state
            ORDER BY DATE_TRUNC('month', o.order_purchase_timestamp)
        ), 0),
        2
    )                                                                   AS mom_growth_pct
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN (
    SELECT order_id, SUM(payment_value) AS payment_value
    FROM order_payments GROUP BY order_id
) pay ON o.order_id = pay.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.customer_state, DATE_TRUNC('month', o.order_purchase_timestamp)
ORDER BY c.customer_state, order_month;


-- Customer demand vs seller supply by state
WITH customer_demand AS (
    SELECT
        c.customer_state                AS state,
        COUNT(DISTINCT o.order_id)      AS orders_placed,
        ROUND(SUM(pay.payment_value), 2) AS demand_revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN (
        SELECT order_id, SUM(payment_value) AS payment_value
        FROM order_payments GROUP BY order_id
    ) pay ON o.order_id = pay.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY c.customer_state
),
seller_supply AS (
    SELECT
        s.seller_state                  AS state,
        COUNT(DISTINCT s.seller_id)     AS seller_count,
        COUNT(DISTINCT oi.order_id)     AS orders_fulfilled,
        ROUND(SUM(oi.price + oi.freight_value), 2) AS supply_revenue
    FROM sellers s
    JOIN order_items oi ON s.seller_id = oi.seller_id
    GROUP BY s.seller_state
)
SELECT
    COALESCE(cd.state, ss.state)        AS state,
    COALESCE(cd.orders_placed, 0)       AS demand_orders,
    COALESCE(cd.demand_revenue, 0)      AS demand_revenue,
    COALESCE(ss.seller_count, 0)        AS local_sellers,
    COALESCE(ss.orders_fulfilled, 0)    AS locally_fulfilled_orders,
    COALESCE(ss.supply_revenue, 0)      AS local_supply_revenue,
    ROUND(
        COALESCE(cd.orders_placed, 0)::NUMERIC /
        NULLIF(COALESCE(ss.seller_count, 0), 0),
        1
    )                                   AS orders_per_local_seller,
    CASE
        WHEN COALESCE(ss.seller_count, 0) = 0            THEN 'No Local Supply'
        WHEN cd.demand_revenue > ss.supply_revenue * 2   THEN 'High Import Dependency'
        WHEN cd.demand_revenue < ss.supply_revenue * 0.5 THEN 'Net Exporter'
        ELSE 'Balanced'
    END                                 AS supply_demand_classification
FROM customer_demand cd
FULL OUTER JOIN seller_supply ss ON cd.state = ss.state
ORDER BY demand_revenue DESC NULLS LAST;


-- Top 20 cities by order volume
SELECT
    c.customer_city                                                     AS city,
    c.customer_state                                                    AS state,
    COUNT(DISTINCT o.order_id)                                          AS total_orders,
    COUNT(DISTINCT c.customer_unique_id)                                AS unique_customers,
    ROUND(SUM(pay.payment_value), 2)                                   AS total_revenue,
    ROUND(AVG(pay.payment_value), 2)                                   AS avg_order_value
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN (
    SELECT order_id, SUM(payment_value) AS payment_value
    FROM order_payments GROUP BY order_id
) pay ON o.order_id = pay.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.customer_city, c.customer_state
ORDER BY total_orders DESC
LIMIT 20;


-- Average order value by Brazilian region
SELECT
    CASE c.customer_state
        WHEN 'SP' THEN 'Southeast' WHEN 'RJ' THEN 'Southeast' WHEN 'MG' THEN 'Southeast'
        WHEN 'ES' THEN 'Southeast'
        WHEN 'RS' THEN 'South'    WHEN 'SC' THEN 'South'     WHEN 'PR' THEN 'South'
        WHEN 'BA' THEN 'Northeast' WHEN 'PE' THEN 'Northeast' WHEN 'CE' THEN 'Northeast'
        WHEN 'MA' THEN 'Northeast' WHEN 'PB' THEN 'Northeast' WHEN 'RN' THEN 'Northeast'
        WHEN 'AL' THEN 'Northeast' WHEN 'SE' THEN 'Northeast' WHEN 'PI' THEN 'Northeast'
        WHEN 'PA' THEN 'North'    WHEN 'AM' THEN 'North'     WHEN 'RO' THEN 'North'
        WHEN 'AC' THEN 'North'    WHEN 'AP' THEN 'North'     WHEN 'RR' THEN 'North'
        WHEN 'TO' THEN 'North'
        WHEN 'DF' THEN 'Central-West' WHEN 'GO' THEN 'Central-West'
        WHEN 'MT' THEN 'Central-West' WHEN 'MS' THEN 'Central-West'
        ELSE 'Other'
    END                                                                 AS region,
    c.customer_state                                                    AS state,
    COUNT(DISTINCT o.order_id)                                          AS total_orders,
    ROUND(AVG(pay.payment_value), 2)                                   AS avg_order_value,
    ROUND(SUM(pay.payment_value), 2)                                   AS total_revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN (
    SELECT order_id, SUM(payment_value) AS payment_value
    FROM order_payments GROUP BY order_id
) pay ON o.order_id = pay.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY region, c.customer_state
ORDER BY region, total_revenue DESC;
