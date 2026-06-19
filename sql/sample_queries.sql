-- =============================================================================
-- Olist E-Commerce — Sample analytical queries
-- Source tables: workspace.gold.*
-- Run in the Databricks SQL Editor (catalog: workspace)
-- =============================================================================

-- =============================================================================
-- 1. Daily revenue by state
-- Answers: "What was the revenue over time and by region?"
-- =============================================================================
SELECT
    order_purchase_date,
    customer_state,
    COUNT(DISTINCT order_id)        AS total_orders,
    SUM(payment_total_value)        AS total_revenue,
    AVG(payment_total_value)        AS avg_order_value
FROM workspace.gold.fact_order_revenue
GROUP BY order_purchase_date, customer_state
ORDER BY order_purchase_date, total_revenue DESC;


-- =============================================================================
-- 2. Total revenue by state (ranking)
-- Answers: "Which states generate the most revenue?"
-- =============================================================================
SELECT
    customer_state,
    total_orders,
    delivered_orders,
    total_revenue,
    avg_order_value,
    avg_delivery_days,
    late_rate
FROM workspace.gold.customer_state_revenue
ORDER BY total_revenue DESC;


-- =============================================================================
-- 3. Late delivery rate by state
-- Answers: "Where are the biggest delivery problems?"
-- Note: is_late = null for undelivered orders — WHERE is_delivered = true
-- ensures only completed orders enter the rate calculation
-- =============================================================================
SELECT
    customer_state,
    delivered_orders,
    late_orders,
    late_rate,
    avg_delivery_days
FROM workspace.gold.customer_state_revenue
ORDER BY late_rate DESC;


-- =============================================================================
-- 4. Average delivery time by state
-- Answers: "What is the average delivery time by region?"
-- =============================================================================
SELECT
    customer_state,
    AVG(delivery_days)              AS avg_delivery_days,
    MIN(delivery_days)              AS min_delivery_days,
    MAX(delivery_days)              AS max_delivery_days,
    COUNT(order_id)                 AS delivered_orders
FROM workspace.gold.fact_order_revenue
WHERE is_delivered = true
GROUP BY customer_state
ORDER BY avg_delivery_days DESC;


-- =============================================================================
-- 5. Orders delivered on time vs late
-- Answers: "What proportion of orders was delivered on time?"
-- =============================================================================
SELECT
    is_late,
    COUNT(order_id)                 AS total_orders,
    ROUND(AVG(delivery_days), 1)    AS avg_delivery_days
FROM workspace.gold.fact_order_revenue
WHERE is_delivered = true
GROUP BY is_late
ORDER BY is_late;


-- =============================================================================
-- 6. Top 10 categories by revenue
-- Answers: "Which categories sell the most?"
-- =============================================================================
SELECT
    product_category_name_english,
    total_orders,
    total_items,
    total_revenue,
    avg_item_price
FROM workspace.gold.product_category_revenue
ORDER BY total_revenue DESC
LIMIT 10;


-- =============================================================================
-- 7. Revenue and volume by payment method
-- Answers: "Which payment method is most commonly used?"
-- =============================================================================
SELECT
    main_payment_type,
    total_orders,
    total_revenue,
    avg_order_value,
    avg_installments
FROM workspace.gold.payment_method_summary
ORDER BY total_orders DESC;


-- =============================================================================
-- 8. Installment vs upfront orders — average ticket
-- Answers: "Do installment orders have a higher average ticket?"
-- Compares credit_card (installments) with boleto and voucher (usually upfront)
-- =============================================================================
SELECT
    main_payment_type,
    avg_order_value,
    avg_installments,
    CASE
        WHEN avg_installments > 1.5 THEN 'installment'
        ELSE 'upfront'
    END AS payment_modality
FROM workspace.gold.payment_method_summary
ORDER BY avg_order_value DESC;


-- =============================================================================
-- 9. Top 10 sellers by revenue
-- Answers: "Which sellers have the highest sales volume?"
-- =============================================================================
SELECT
    seller_id,
    seller_state,
    total_orders,
    total_items,
    total_revenue,
    avg_item_price,
    avg_delivery_days,
    late_rate
FROM workspace.gold.seller_performance
ORDER BY total_revenue DESC
LIMIT 10;


-- =============================================================================
-- 10. Sellers with the highest late rate (minimum 50 orders)
-- Answers: "Which sellers have the worst delivery rates?"
-- Minimum 50 orders filter avoids distortion from sellers with few orders
-- =============================================================================
SELECT
    seller_id,
    seller_state,
    total_orders,
    late_order_count,
    late_rate,
    avg_delivery_days
FROM workspace.gold.seller_performance
WHERE total_orders >= 50
ORDER BY late_rate DESC
LIMIT 10;


-- =============================================================================
-- 11. Monthly revenue trend
-- Answers: "How did revenue evolve over the months?"
-- =============================================================================
SELECT
    DATE_TRUNC('month', order_purchase_date)    AS month,
    COUNT(DISTINCT order_id)                    AS total_orders,
    SUM(payment_total_value)                    AS total_revenue,
    AVG(payment_total_value)                    AS avg_order_value
FROM workspace.gold.fact_order_revenue
GROUP BY DATE_TRUNC('month', order_purchase_date)
ORDER BY month;


-- =============================================================================
-- 12. Order distribution by status
-- Overall view: how many orders at each lifecycle stage
-- =============================================================================
SELECT
    order_status,
    COUNT(order_id)                 AS total_orders,
    ROUND(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER (), 2) AS pct
FROM workspace.gold.fact_order_revenue
GROUP BY order_status
ORDER BY total_orders DESC;


-- =============================================================================
-- 13. Quality check results (04_data_quality_checks.py)
-- Overview of the audit pipeline: which rules passed and which failed
-- =============================================================================
SELECT
    table_name,
    rule_name,
    total_records,
    invalid_records,
    invalid_pct,
    status,
    checked_at
FROM workspace.gold.data_quality_summary
ORDER BY table_name, rule_name;


-- =============================================================================
-- 14. Failed rules — detail for investigation
-- =============================================================================
SELECT
    table_name,
    rule_name,
    invalid_records,
    invalid_pct
FROM workspace.gold.data_quality_summary
WHERE status = 'FAIL'
ORDER BY invalid_records DESC;
