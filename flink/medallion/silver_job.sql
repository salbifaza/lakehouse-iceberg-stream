-- Silver layer: cleaned, typed, conformed, and joined entities, continuously
-- derived from the bronze changelog. Regular (stateful) joins, not lookup
-- joins: a regular join re-emits rows when *either* side changes (e.g. a
-- category rename updates every order line in that category), which keeps
-- silver an exact function of bronze. See README "Streaming medallion".
SET 'pipeline.name' = 'Medallion: bronze -> silver';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO silver.customers
SELECT
  customer_id,
  LOWER(TRIM(email)),
  CONCAT_WS(' ', TRIM(first_name), TRIM(last_name)),
  UPPER(TRIM(country)),
  created_at,
  updated_at
FROM bronze.customers;

INSERT INTO silver.products
SELECT
  p.product_id,
  p.sku,
  TRIM(p.name),
  p.category_id,
  c.name,
  CAST(p.price_cents AS BIGINT),
  CAST(CAST(p.price_cents AS DECIMAL(12, 2)) / 100 AS DECIMAL(12, 2))
FROM bronze.products AS p
JOIN bronze.categories AS c ON p.category_id = c.category_id;

INSERT INTO silver.orders
SELECT
  order_id,
  customer_id,
  LOWER(TRIM(status)),
  CAST(order_total_cents AS BIGINT),
  CAST(CAST(order_total_cents AS DECIMAL(12, 2)) / 100 AS DECIMAL(12, 2)),
  DATE_FORMAT(created_at, 'yyyy-MM-dd'),
  created_at,
  updated_at
FROM bronze.orders;

INSERT INTO silver.order_lines
SELECT
  oi.order_item_id,
  oi.order_id,
  o.customer_id,
  LOWER(TRIM(o.status)),
  DATE_FORMAT(o.created_at, 'yyyy-MM-dd'),
  oi.product_id,
  p.sku,
  TRIM(p.name),
  p.category_id,
  c.name,
  oi.quantity,
  CAST(oi.unit_price_cents AS BIGINT),
  CAST(oi.quantity AS BIGINT) * oi.unit_price_cents
FROM bronze.order_items AS oi
JOIN bronze.orders     AS o ON oi.order_id   = o.order_id
JOIN bronze.products   AS p ON oi.product_id = p.product_id
JOIN bronze.categories AS c ON p.category_id = c.category_id;

INSERT INTO silver.payments
SELECT
  payment_id,
  order_id,
  CAST(amount_cents AS BIGINT),
  CAST(CAST(amount_cents AS DECIMAL(12, 2)) / 100 AS DECIMAL(12, 2)),
  LOWER(TRIM(`method`)),
  LOWER(TRIM(status)),
  processed_at
FROM bronze.payments;

END;
