SET search_path TO olist;

WITH seller_base AS (
    SELECT
        s.seller_id, s.seller_city, s.seller_state,
        COUNT(DISTINCT oi.order_id)                                     AS total_orders,
        COUNT(DISTINCT oi.product_id)                                   AS unique_products,
        ROUND(SUM(oi.price + oi.freight_value), 2)                     AS total_revenue,
        ROUND(AVG(oi.price), 2)                                        AS avg_item_price,
        ROUND(AVG(oi.freight_value), 2)                                AS avg_freight_value
    FROM sellers s
    JOIN order_items oi ON s.seller_id = oi.seller_id
    GROUP BY s.seller_id, s.seller_city, s.seller_state
),
seller_delivery AS (
    SELECT
        oi.seller_id,
        COUNT(DISTINCT o.order_id)                                      AS delivered_orders,
        COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date IS NOT NULL
              AND o.order_delivered_customer_date::DATE <= o.order_estimated_delivery_date
        )                                                               AS on_time_orders,
        ROUND(
            COUNT(DISTINCT o.order_id) FILTER (
                WHERE o.order_delivered_customer_date IS NOT NULL
                  AND o.order_delivered_customer_date::DATE <= o.order_estimated_delivery_date
            ) * 100.0 /
            NULLIF(COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'delivered'), 0),
            2
        )                                                               AS on_time_rate_pct,
        ROUND(AVG(
            EXTRACT(EPOCH FROM (
                o.order_delivered_customer_date - o.order_purchase_timestamp
            )) / 86400.0
        ) FILTER (WHERE o.order_delivered_customer_date IS NOT NULL), 1) AS avg_lead_time_days
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY oi.seller_id
),
seller_reviews AS (
    SELECT
        oi.seller_id,
        ROUND(AVG(r.review_score::NUMERIC), 2)                         AS avg_review_score,
        COUNT(r.review_id)                                             AS total_reviews
    FROM order_items oi
    JOIN order_reviews r ON oi.order_id = r.order_id
    GROUP BY oi.seller_id
),
seller_combined AS (
    SELECT
        b.seller_id, b.seller_city, b.seller_state,
        b.total_orders, b.unique_products, b.total_revenue,
        b.avg_item_price, b.avg_freight_value,
        COALESCE(d.on_time_rate_pct, 0)   AS on_time_rate_pct,
        COALESCE(d.avg_lead_time_days, 0) AS avg_lead_time_days,
        COALESCE(r.avg_review_score, 0)   AS avg_review_score,
        COALESCE(r.total_reviews, 0)      AS total_reviews
    FROM seller_base b
    LEFT JOIN seller_delivery d ON b.seller_id = d.seller_id
    LEFT JOIN seller_reviews r  ON b.seller_id = r.seller_id
),
seller_normalized AS (
    SELECT *,
        ROUND(
            (total_revenue - MIN(total_revenue) OVER ()) * 100.0 /
            NULLIF(MAX(total_revenue) OVER () - MIN(total_revenue) OVER (), 0),
            2
        )                                                               AS revenue_score_normalized,
        ROUND((avg_review_score - 1) * 25.0, 2)                       AS review_score_normalized
    FROM seller_combined
    WHERE total_orders >= 5
),
seller_scored AS (
    SELECT *,
        ROUND(
            (on_time_rate_pct         * 0.40) +
            (review_score_normalized  * 0.30) +
            (revenue_score_normalized * 0.30),
            2
        )                                                               AS composite_score,
        CASE
            WHEN total_orders >= 200 THEN 'High Volume'
            WHEN total_orders >= 50  THEN 'Mid Volume'
            ELSE                          'Low Volume'
        END                                                             AS volume_tier
    FROM seller_normalized
)

-- Full seller scorecard
SELECT
    seller_id, seller_city, seller_state, total_orders, unique_products,
    total_revenue, avg_item_price, on_time_rate_pct, avg_lead_time_days,
    avg_review_score, total_reviews, revenue_score_normalized,
    review_score_normalized, composite_score, volume_tier,
    RANK() OVER (ORDER BY composite_score DESC) AS performance_rank
FROM seller_scored
ORDER BY composite_score DESC;


-- Top 10 sellers
WITH seller_scored AS (
    SELECT
        b.seller_id, b.seller_city, b.seller_state, b.total_orders,
        COALESCE(d.on_time_rate_pct, 0)  AS on_time_rate_pct,
        COALESCE(r.avg_review_score, 0)  AS avg_review_score,
        b.total_revenue,
        ROUND(
            (COALESCE(d.on_time_rate_pct, 0) * 0.40) +
            (((COALESCE(r.avg_review_score, 0) - 1) * 25.0) * 0.30) +
            (
                (b.total_revenue - MIN(b.total_revenue) OVER ()) * 100.0 /
                NULLIF(MAX(b.total_revenue) OVER () - MIN(b.total_revenue) OVER (), 0)
                * 0.30
            ),
            2
        )                                AS composite_score
    FROM (
        SELECT s.seller_id, s.seller_city, s.seller_state,
               COUNT(DISTINCT oi.order_id) AS total_orders,
               ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue
        FROM sellers s
        JOIN order_items oi ON s.seller_id = oi.seller_id
        GROUP BY s.seller_id, s.seller_city, s.seller_state
        HAVING COUNT(DISTINCT oi.order_id) >= 5
    ) b
    LEFT JOIN (
        SELECT oi.seller_id,
               ROUND(COUNT(DISTINCT o.order_id) FILTER (
                   WHERE o.order_delivered_customer_date IS NOT NULL
                     AND o.order_delivered_customer_date::DATE <= o.order_estimated_delivery_date
               ) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS on_time_rate_pct
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY oi.seller_id
    ) d ON b.seller_id = d.seller_id
    LEFT JOIN (
        SELECT oi.seller_id, ROUND(AVG(r.review_score::NUMERIC), 2) AS avg_review_score
        FROM order_items oi
        JOIN order_reviews r ON oi.order_id = r.order_id
        GROUP BY oi.seller_id
    ) r ON b.seller_id = r.seller_id
)
SELECT seller_id, seller_city, seller_state, total_orders,
       on_time_rate_pct, avg_review_score, total_revenue, composite_score
FROM seller_scored
ORDER BY composite_score DESC
LIMIT 10;


-- Bottom 10 sellers
SELECT
    seller_id, seller_city, seller_state, total_orders,
    on_time_rate_pct, avg_review_score, composite_score,
    CASE
        WHEN on_time_rate_pct < 70 THEN 'Poor Delivery'
        WHEN avg_review_score < 3  THEN 'Poor Reviews'
        ELSE 'Low Revenue'
    END AS primary_issue
FROM seller_scored
ORDER BY composite_score ASC
LIMIT 10;


-- Seller performance by state
SELECT
    s.seller_state,
    COUNT(DISTINCT s.seller_id)                                         AS seller_count,
    COUNT(DISTINCT oi.order_id)                                         AS total_orders,
    ROUND(SUM(oi.price + oi.freight_value), 2)                         AS total_revenue,
    ROUND(AVG(r.review_score::NUMERIC), 2)                             AS avg_review_score,
    ROUND(
        COUNT(DISTINCT o.order_id) FILTER (
            WHERE o.order_delivered_customer_date IS NOT NULL
              AND o.order_delivered_customer_date::DATE <= o.order_estimated_delivery_date
        ) * 100.0 /
        NULLIF(COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status = 'delivered'), 0),
        2
    )                                                                   AS state_on_time_rate_pct
FROM sellers s
JOIN order_items oi ON s.seller_id = oi.seller_id
JOIN orders o ON oi.order_id = o.order_id
LEFT JOIN order_reviews r ON o.order_id = r.order_id
GROUP BY s.seller_state
ORDER BY total_revenue DESC;
