
-- Запрос 1. Распределение клиентов по сферам
SELECT job_industry_category,
       COUNT(*) AS customers_cnt
FROM customers
WHERE job_industry_category IS NOT NULL
GROUP BY job_industry_category
ORDER BY customers_cnt DESC;

-- Запрос 2. Доход по месяцам и сферам деятельности
WITH order_amounts AS (
    SELECT
        o.order_id,
        o.customer_id,
        SUM(oi.quantity * oi.item_list_price_at_sale) AS order_revenue,
        DATE_PART('year', o.order_date) AS order_year,
        DATE_PART('month', o.order_date) AS order_month
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    GROUP BY o.order_id, o.customer_id, o.order_date
)
SELECT
    oa.order_year AS year,
    oa.order_month AS month,
    c.job_industry_category,
    SUM(oa.order_revenue) AS total_revenue
FROM order_amounts oa
JOIN customers c ON c.customer_id = oa.customer_id
GROUP BY
    oa.order_year,
    oa.order_month,
    c.job_industry_category
ORDER BY
    year,
    month,
    job_industry_category;

-- Запрос 3. Онлайн-заказы IT-клиентов по брендам
SELECT
    p.product_line AS brand,
    COUNT(o.order_id) AS orders_cnt
FROM products p
LEFT JOIN order_items oi ON oi.product_id = p.product_id
LEFT JOIN orders o ON o.order_id = oi.order_id 
LEFT JOIN customers c ON c.customer_id = o.customer_id
    AND c.job_industry_category = 'IT'
    AND o.online_order = TRUE
GROUP BY p.product_line
ORDER BY orders_cnt DESC;

-- Запрос 4. МЕТРИКИ ПО КЛИЕНТАМ
-- total revenue, max, min, average, orders count

-- 1. Удаляем временную таблицу
DROP TABLE IF EXISTS temp_order_amounts;

-- 2. Создаём временную таблицу со всеми суммами заказов
CREATE TEMP TABLE temp_order_amounts AS
SELECT 
    o.order_id,
    o.customer_id,
    SUM(oi.quantity * oi.item_list_price_at_sale) AS order_revenue
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY o.order_id, o.customer_id;

-- 3. Основной расчёт по клиентам
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COALESCE(SUM(t.order_revenue), 0) AS total_revenue,
    COALESCE(MAX(t.order_revenue), 0) AS max_order_revenue,
    COALESCE(MIN(t.order_revenue), 0) AS min_order_revenue,
    COALESCE(COUNT(t.order_id), 0) AS orders_cnt,
    COALESCE(AVG(t.order_revenue), 0) AS avg_order_revenue
FROM customers c
LEFT JOIN temp_order_amounts t ON t.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name
ORDER BY
    total_revenue DESC,
    orders_cnt DESC;

-- Запрос 4B. Метрики по клиентам через оконные функции

WITH order_revenue AS (
    SELECT
        o.order_id,
        o.customer_id,
        SUM(oi.quantity * oi.item_list_price_at_sale) AS order_revenue
    FROM orders o
    LEFT JOIN order_items oi ON oi.order_id = o.order_id
    GROUP BY o.order_id, o.customer_id
)

SELECT DISTINCT
    c.customer_id,
    c.first_name,
    c.last_name,

    COALESCE(SUM(order_revenue) OVER (PARTITION BY c.customer_id), 0) AS total_revenue,
    COALESCE(MAX(order_revenue) OVER (PARTITION BY c.customer_id), 0) AS max_order_revenue,
    COALESCE(MIN(order_revenue) OVER (PARTITION BY c.customer_id), 0) AS min_order_revenue,
    COALESCE(COUNT(order_revenue) OVER (PARTITION BY c.customer_id), 0) AS orders_cnt,
    COALESCE(AVG(order_revenue) OVER (PARTITION BY c.customer_id), 0) AS avg_order_revenue

FROM customers c
LEFT JOIN order_revenue r ON r.customer_id = c.customer_id

ORDER BY
    total_revenue DESC,
    orders_cnt DESC;

-- Запрос 5. Топ-3 клиентов с минимальной и топ-3 с максимальной суммой заказов

WITH customer_sums AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) AS total_revenue
    FROM customers c
    LEFT JOIN orders o ON o.customer_id = c.customer_id
    LEFT JOIN order_items oi ON oi.order_id = o.order_id
    GROUP BY c.customer_id, c.first_name, c.last_name
),

ranked AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        total_revenue,
        RANK() OVER (ORDER BY total_revenue ASC) AS min_rank,
        RANK() OVER (ORDER BY total_revenue DESC) AS max_rank
    FROM customer_sums
)

SELECT
    customer_id,
    first_name,
    last_name,
    total_revenue
FROM ranked
WHERE min_rank <= 3 OR max_rank <= 3
ORDER BY total_revenue;

-- Запрос 6. Только вторые транзакции клиентов

WITH ordered_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_date
        ) AS rn
    FROM orders o
)

SELECT
    o.customer_id,
    c.first_name,
    c.last_name,
    o.order_id,
    o.order_date
FROM ordered_orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.rn = 2
ORDER BY o.customer_id;

-- Запрос 7. Максимальный интервал между двумя заказами по каждому клиенту

WITH ordered_dates AS (
    SELECT
        o.customer_id,
        o.order_date,
        LAG(o.order_date) OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_date
        ) AS prev_date
    FROM orders o
),

intervals AS (
    SELECT
        customer_id,
        order_date,
        prev_date,
        CASE
            WHEN prev_date IS NOT NULL
            THEN (order_date - prev_date)
            ELSE NULL
        END AS days_diff
    FROM ordered_dates
),

max_intervals AS (
    SELECT
        customer_id,
        MAX(days_diff) AS max_interval
    FROM intervals
    GROUP BY customer_id
    HAVING MAX(days_diff) IS NOT NULL   -- исключаем клиентов с 1 заказом
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.job_industry_category,
    m.max_interval
FROM max_intervals m
JOIN customers c ON c.customer_id = m.customer_id
ORDER BY m.max_interval DESC;

-- Запрос 8. Топ-5 клиентов по общему доходу в каждом сегменте благосостояния

-- 1. Считаем общий доход каждого клиента
WITH customer_revenue AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.wealth_segment,
        COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) AS total_revenue
    FROM customers c
    LEFT JOIN orders o ON o.customer_id = c.customer_id
    LEFT JOIN order_items oi ON oi.order_id = o.order_id
    GROUP BY
        c.customer_id,
        c.first_name,
        c.last_name,
        c.wealth_segment
),

-- 2. Ранжируем клиентов внутри каждого сегмента по доходу
ranked AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        wealth_segment,
        total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY wealth_segment
            ORDER BY total_revenue DESC
        ) AS rn
    FROM customer_revenue
)

-- 3. Выбираем топ-5 в каждом сегменте
SELECT
    customer_id,
    first_name,
    last_name,
    wealth_segment,
    total_revenue
FROM ranked
WHERE rn <= 5
ORDER BY
    wealth_segment,
    total_revenue DESC;