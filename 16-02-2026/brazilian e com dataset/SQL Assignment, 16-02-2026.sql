-- Setting datestyle to ISO, MDY
SET datestyle TO ISO, MDY;

-- Creating required rables
CREATE TABLE olist_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE olist_order_items (
    order_id TEXT,
    order_item_id INT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMP,
    price NUMERIC,
    freight_value NUMERIC
);

CREATE TABLE olist_customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix INT,
    customer_city TEXT,
    customer_state TEXT
);

CREATE TABLE olist_products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

-- Verifying if the .csv files have been loaded properly or not
SELECT COUNT(*) FROM olist_orders;
SELECT COUNT(*) FROM olist_customers;
SELECT COUNT(*) FROM olist_order_items;
SELECT COUNT(*) FROM olist_products;

-- 
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'olist_products';

-- PART 1: High-Growth States + Top Categories
-- Query 1: Monthly orders by state
WITH monthly_orders AS (
    SELECT
        c.customer_state,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM olist_orders o
    JOIN olist_customers c
    ON o.customer_id = c.customer_id
    WHERE EXTRACT(YEAR FROM o.order_purchase_timestamp) = 2017
    GROUP BY 1,2
)
SELECT *
FROM monthly_orders
ORDER BY customer_state, month;

-- Query 2: Identify High-Growth States
WITH monthly_orders AS (
    SELECT
        c.customer_state,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM olist_orders o
    JOIN olist_customers c
    ON o.customer_id = c.customer_id
    WHERE EXTRACT(YEAR FROM o.order_purchase_timestamp) = 2017
    GROUP BY 1,2
),
nov_dec AS (
    SELECT
        customer_state,
        MAX(CASE WHEN month = '2017-11-01' THEN order_count END) AS nov_orders,
        MAX(CASE WHEN month = '2017-12-01' THEN order_count END) AS dec_orders
    FROM monthly_orders
    GROUP BY 1
)
SELECT *,
       (dec_orders - nov_orders) * 100.0 / nov_orders AS growth_percent
FROM nov_dec
WHERE nov_orders IS NOT NULL
AND dec_orders IS NOT NULL
AND (dec_orders - nov_orders) * 100.0 / nov_orders > 5;

-- Query 3: Top 3 product categories by revenue in those states
WITH high_growth_states AS (
    WITH monthly_orders AS (
        SELECT
            c.customer_state,
            DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
            COUNT(DISTINCT o.order_id) AS order_count
        FROM olist_orders o
        JOIN olist_customers c
        ON o.customer_id = c.customer_id
        WHERE EXTRACT(YEAR FROM o.order_purchase_timestamp) = 2017
        GROUP BY 1,2
    ),
    nov_dec AS (
        SELECT
            customer_state,
            MAX(CASE WHEN month = '2017-11-01' THEN order_count END) AS nov_orders,
            MAX(CASE WHEN month = '2017-12-01' THEN order_count END) AS dec_orders
        FROM monthly_orders
        GROUP BY 1
    )
    SELECT customer_state
    FROM nov_dec
    WHERE nov_orders IS NOT NULL
    AND dec_orders IS NOT NULL
    AND (dec_orders - nov_orders) * 100.0 / nov_orders > 5
),
revenue_data AS (
    SELECT
        c.customer_state,
        p.product_category_name,
        SUM(oi.price) AS revenue
    FROM olist_orders o
    JOIN olist_customers c ON o.customer_id = c.customer_id
    JOIN olist_order_items oi ON o.order_id = oi.order_id
    JOIN olist_products p ON oi.product_id = p.product_id
    WHERE EXTRACT(YEAR FROM o.order_purchase_timestamp) = 2017
    AND c.customer_state IN (SELECT customer_state FROM high_growth_states)
    GROUP BY 1,2
),
ranked AS (
    SELECT *,
           RANK() OVER (
               PARTITION BY customer_state
               ORDER BY revenue DESC
           ) AS rnk
    FROM revenue_data
)
SELECT *
FROM ranked
WHERE rnk <= 3;

-- PART 2: First Purchase Behaviour
-- Query 4: Segment customers
WITH customer_stats AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.price) AS total_spent
    FROM olist_orders o
    JOIN olist_customers c
    ON o.customer_id = c.customer_id
    JOIN olist_order_items oi
    ON o.order_id = oi.order_id
    GROUP BY 1
),
segmented AS (
    SELECT *,
        CASE
            WHEN total_orders >= 2 THEN 'High Value'
            WHEN total_orders = 1 AND total_spent < 1000 THEN 'Low Value'
        END AS segment
    FROM customer_stats
)
SELECT * FROM segmented;

-- Query 5: First order of each customer
WITH first_orders AS (
    SELECT
        c.customer_unique_id,
        o.order_id,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp
        ) AS rn
    FROM olist_orders o
    JOIN olist_customers c
    ON o.customer_id = c.customer_id
)
SELECT *
FROM first_orders
WHERE rn = 1;

-- Query 6: First purchase category
WITH customer_stats AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.price) AS total_spent
    FROM olist_orders o
    JOIN olist_customers c
    ON o.customer_id = c.customer_id
    JOIN olist_order_items oi
    ON o.order_id = oi.order_id
    GROUP BY 1
),
segmented AS (
    SELECT *,
        CASE
            WHEN total_orders >= 2 THEN 'High Value'
            WHEN total_orders = 1 AND total_spent < 1000 THEN 'Low Value'
        END AS segment
    FROM customer_stats
),
first_orders AS (
    SELECT
        c.customer_unique_id,
        o.order_id,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp
        ) AS rn
    FROM olist_orders o
    JOIN olist_customers c
    ON o.customer_id = c.customer_id
),
first_purchase AS (
    SELECT
        fo.customer_unique_id,
        fo.order_id
    FROM first_orders fo
    WHERE rn = 1
),
first_categories AS (
    SELECT
        s.segment,
        p.product_category_name,
        COUNT(*) AS cnt
    FROM first_purchase fp
    JOIN segmented s
    ON fp.customer_unique_id = s.customer_unique_id
    JOIN olist_order_items oi
    ON fp.order_id = oi.order_id
    JOIN olist_products p
    ON oi.product_id = p.product_id
    WHERE s.segment IS NOT NULL
    GROUP BY 1,2
),
ranked AS (
    SELECT *,
           RANK() OVER (
               PARTITION BY segment
               ORDER BY cnt DESC
           ) AS rnk
    FROM first_categories
)
SELECT *
FROM ranked
WHERE rnk <= 3;
