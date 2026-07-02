-- ── India Warehouse Digital Twin ───────────────────────────────────────────────
-- Databricks SQL analytical queries on Gold layer
-- ──────────────────────────────────────────────────────────────────────────────

USE CATALOG warehouse_catalog;
USE SCHEMA gold;

-- ── 1. Overall health summary across all warehouses ───────────────────────────
SELECT
    warehouse_status,
    COUNT(*) AS warehouse_count,
    ROUND(AVG(operational_health_score), 4) AS avg_health_score
FROM gold_warehouse_health
GROUP BY warehouse_status
ORDER BY warehouse_count DESC;


-- ── 2. Top 10 healthiest warehouses ───────────────────────────────────────────
SELECT
    warehouse_id,
    warehouse_city,
    warehouse_state,
    operational_health_score,
    warehouse_status
FROM gold_warehouse_health
ORDER BY operational_health_score DESC
LIMIT 10;


-- ── 3. At-risk warehouses needing immediate attention ─────────────────────────
SELECT
    warehouse_id,
    warehouse_city,
    warehouse_state,
    avg_fulfillment_rate,
    avg_capacity_pct,
    inventory_health_score,
    operational_health_score
FROM gold_warehouse_health
WHERE warehouse_status = 'AT RISK'
ORDER BY operational_health_score ASC;


-- ── 4. State-wise average health score ────────────────────────────────────────
SELECT
    warehouse_state,
    COUNT(*) AS total_warehouses,
    ROUND(AVG(operational_health_score), 4) AS avg_health_score,
    SUM(CASE WHEN warehouse_status = 'AT RISK' THEN 1 ELSE 0 END) AS at_risk_count
FROM gold_warehouse_health
GROUP BY warehouse_state
ORDER BY avg_health_score ASC;


-- ── 5. Warehouses with high capacity but low fulfillment ──────────────────────
SELECT
    warehouse_id,
    warehouse_city,
    warehouse_state,
    avg_capacity_pct,
    avg_fulfillment_rate,
    operational_health_score
FROM gold_warehouse_health
WHERE avg_capacity_pct > 80
  AND avg_fulfillment_rate < 0.75
ORDER BY avg_fulfillment_rate ASC;


-- ── 6. Fulfillment time performance by state ──────────────────────────────────
SELECT
    warehouse_state,
    ROUND(AVG(avg_fulfillment_time_hrs), 2) AS avg_fulfillment_hours,
    COUNT(*) AS warehouse_count
FROM gold_warehouse_health
GROUP BY warehouse_state
ORDER BY avg_fulfillment_hours ASC;