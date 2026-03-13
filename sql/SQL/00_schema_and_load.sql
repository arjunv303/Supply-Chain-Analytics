CREATE SCHEMA IF NOT EXISTS olist;
SET search_path TO olist;

DROP TABLE IF EXISTS order_items          CASCADE;
DROP TABLE IF EXISTS order_payments       CASCADE;
DROP TABLE IF EXISTS order_reviews        CASCADE;
DROP TABLE IF EXISTS orders               CASCADE;
DROP TABLE IF EXISTS customers            CASCADE;
DROP TABLE IF EXISTS sellers              CASCADE;
DROP TABLE IF EXISTS products             CASCADE;
DROP TABLE IF EXISTS product_categories   CASCADE;
DROP TABLE IF EXISTS geolocation          CASCADE;

CREATE TABLE customers (
    customer_id             VARCHAR(50) PRIMARY KEY,
    customer_unique_id      VARCHAR(50) NOT NULL,
    customer_zip_code       VARCHAR(10),
    customer_city           VARCHAR(100),
    customer_state          CHAR(2)
);

CREATE TABLE sellers (
    seller_id               VARCHAR(50) PRIMARY KEY,
    seller_zip_code         VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            CHAR(2)
);

CREATE TABLE product_categories (
    category_name_portuguese VARCHAR(100) PRIMARY KEY,
    category_name_english    VARCHAR(100)
);

CREATE TABLE products (
    product_id              VARCHAR(50) PRIMARY KEY,
    product_category_name   VARCHAR(100),
    product_name_length     INT,
    product_description_length INT,
    product_photos_qty      INT,
    product_weight_g        NUMERIC(10,2),
    product_length_cm       NUMERIC(10,2),
    product_height_cm       NUMERIC(10,2),
    product_width_cm        NUMERIC(10,2)
);

CREATE TABLE orders (
    order_id                        VARCHAR(50) PRIMARY KEY,
    customer_id                     VARCHAR(50) REFERENCES customers(customer_id),
    order_status                    VARCHAR(30),
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_carrier_delivery_date     TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   DATE
);

CREATE TABLE order_items (
    order_id            VARCHAR(50) REFERENCES orders(order_id),
    order_item_id       INT,
    product_id          VARCHAR(50) REFERENCES products(product_id),
    seller_id           VARCHAR(50) REFERENCES sellers(seller_id),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10,2),
    freight_value       NUMERIC(10,2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payments (
    order_id            VARCHAR(50) REFERENCES orders(order_id),
    payment_sequential  INT,
    payment_type        VARCHAR(30),
    payment_installments INT,
    payment_value       NUMERIC(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE order_reviews (
    review_id                   VARCHAR(50),
    order_id                    VARCHAR(50) REFERENCES orders(order_id),
    review_score                SMALLINT CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title        TEXT,
    review_comment_message      TEXT,
    review_creation_date        TIMESTAMP,
    review_answer_timestamp     TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);

CREATE TABLE geolocation (
    geolocation_zip_code    VARCHAR(10),
    geolocation_lat         NUMERIC(10,6),
    geolocation_lng         NUMERIC(10,6),
    geolocation_city        VARCHAR(100),
    geolocation_state       CHAR(2)
);

CREATE INDEX idx_geo_zip ON geolocation(geolocation_zip_code);

-- Update file paths before running
COPY customers FROM '/path/to/data/olist_customers_dataset.csv' CSV HEADER ENCODING 'UTF8';
COPY sellers FROM '/path/to/data/olist_sellers_dataset.csv' CSV HEADER ENCODING 'UTF8';
COPY product_categories FROM '/path/to/data/olist_product_category_name_translation.csv' CSV HEADER ENCODING 'UTF8';
COPY products FROM '/path/to/data/olist_products_dataset.csv' CSV HEADER ENCODING 'UTF8';
COPY orders FROM '/path/to/data/olist_orders_dataset.csv' CSV HEADER ENCODING 'UTF8';
COPY order_items FROM '/path/to/data/olist_order_items_dataset.csv' CSV HEADER ENCODING 'UTF8';
COPY order_payments FROM '/path/to/data/olist_order_payments_dataset.csv' CSV HEADER ENCODING 'UTF8';
COPY order_reviews FROM '/path/to/data/olist_order_reviews_dataset.csv' CSV HEADER ENCODING 'UTF8';
COPY geolocation FROM '/path/to/data/olist_geolocation_dataset.csv' CSV HEADER ENCODING 'UTF8';

SELECT 'customers'         AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL SELECT 'sellers',          COUNT(*) FROM sellers
UNION ALL SELECT 'product_categories', COUNT(*) FROM product_categories
UNION ALL SELECT 'products',         COUNT(*) FROM products
UNION ALL SELECT 'orders',           COUNT(*) FROM orders
UNION ALL SELECT 'order_items',      COUNT(*) FROM order_items
UNION ALL SELECT 'order_payments',   COUNT(*) FROM order_payments
UNION ALL SELECT 'order_reviews',    COUNT(*) FROM order_reviews
UNION ALL SELECT 'geolocation',      COUNT(*) FROM geolocation
ORDER BY table_name;
