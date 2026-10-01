-- Gold layer: business aggregates over silver, maintained continuously.
-- Every aggregate here is retraction-safe, so an UPDATE or DELETE in
-- Postgres corrects the affected gold rows in place. No batch recompute.
SET 'pipeline.name' = 'Medallion: silver -> gold';

EXECUTE STATEMENT SET
BEGIN

-- Revenue excludes cancelled orders; cancelling an order retracts its lines.
INSERT INTO gold.daily_revenue_by_category
SELECT
  order_date,
  category_id,
  MAX(category_name),
  COUNT(DISTINCT order_id),
  SUM(CAST(quantity AS BIGINT)),
  SUM(line_total_cents),
  CAST(CAST(SUM(line_total_cents) AS DECIMAL(18, 2)) / 100 AS DECIMAL(18, 2))
FROM silver.order_lines
WHERE order_status <> 'cancelled'
GROUP BY order_date, category_id;

INSERT INTO gold.customer_lifetime_value
SELECT
  c.customer_id,
  c.email,
  c.country,
  a.order_count,
  a.lifetime_value_cents,
  CAST(CAST(a.lifetime_value_cents AS DECIMAL(18, 2)) / 100 AS DECIMAL(18, 2)),
  a.first_order_at,
  a.last_order_at
FROM (
  SELECT
    customer_id,
    COUNT(*)               AS order_count,
    SUM(order_total_cents) AS lifetime_value_cents,
    MIN(created_at)        AS first_order_at,
    MAX(created_at)        AS last_order_at
  FROM silver.orders
  WHERE status <> 'cancelled'
  GROUP BY customer_id
) AS a
JOIN silver.customers AS c ON a.customer_id = c.customer_id;

INSERT INTO gold.order_status_summary
SELECT
  status,
  COUNT(*),
  SUM(order_total_cents),
  CAST(CAST(SUM(order_total_cents) AS DECIMAL(18, 2)) / 100 AS DECIMAL(18, 2))
FROM silver.orders
GROUP BY status;

END;
