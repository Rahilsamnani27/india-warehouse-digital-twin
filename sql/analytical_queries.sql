-- ── Indian E-Commerce Analytics ────────────────────────────────────────────────
-- Databricks SQL queries on Gold layer tables
-- ──────────────────────────────────────────────────────────────────────────────

USE CATALOG workspace;
USE SCHEMA default;

-- ── 1. Top 10 revenue generating categories ───────────────────────────────────
SELECT
    Category,
    SUM(total_revenue) AS total_revenue,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM gold_revenue_by_category_city
GROUP BY Category
ORDER BY total_revenue DESC
LIMIT 10;


-- ── 2. Top 10 cities by revenue ───────────────────────────────────────────────
SELECT
    City,
    SUM(total_revenue) AS total_revenue,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM gold_revenue_by_category_city
GROUP BY City
ORDER BY total_revenue DESC
LIMIT 10;


-- ── 3. Top 10 high value customers ───────────────────────────────────────────
SELECT
    Customer_ID,
    Name,
    City,
    Membership_label,
    total_spend,
    total_orders,
    total_returns,
    customer_segment
FROM gold_customer_performance
WHERE customer_segment = 'HIGH VALUE'
ORDER BY total_spend DESC
LIMIT 10;


-- ── 4. Payment failure rate by payment mode ───────────────────────────────────
SELECT
    Payment_Mode,
    total_transactions,
    successful_payments,
    failed_payments,
    failure_rate_pct,
    total_refunds
FROM gold_payment_analytics
ORDER BY failure_rate_pct DESC;


-- ── 5. Critical stock products ────────────────────────────────────────────────
SELECT
    Product_ID,
    Product_Name,
    Category,
    Brand,
    Stock_Quantity,
    Reorder_Level,
    days_of_stock_remaining,
    stock_status
FROM gold_stock_risk
WHERE stock_status = 'CRITICAL'
ORDER BY days_of_stock_remaining ASC;


-- ── 6. Revenue vs discount by category ───────────────────────────────────────
SELECT
    Category,
    SUM(total_revenue) AS total_revenue,
    SUM(total_discount_given) AS total_discount,
    ROUND(SUM(total_discount_given) / SUM(total_revenue) * 100, 2) AS discount_rate_pct
FROM gold_revenue_by_category_city
GROUP BY Category
ORDER BY discount_rate_pct DESC;


-- ── 7. State wise revenue summary ─────────────────────────────────────────────
SELECT
    cp.State,
    COUNT(DISTINCT cp.Customer_ID) AS total_customers,
    ROUND(SUM(cp.total_spend), 2) AS total_revenue,
    ROUND(AVG(cp.total_spend), 2) AS avg_customer_spend
FROM gold_customer_performance cp
GROUP BY cp.State
ORDER BY total_revenue DESC;