-- Silver layer tables. Run by run-medallion.sh only when the `silver`
-- database has none of them yet: with table.datalake.enabled, Fluss 0.9.1
-- rejects CREATE TABLE IF NOT EXISTS once the tiered Iceberg table exists
-- ("Table ... already exists"), so these DDLs are not safely re-runnable.
--
-- order_date is STRING (yyyy-MM-dd), not DATE: Fluss 0.9.1's Iceberg tiering
-- writer hands Iceberg a date object where Iceberg's DATE expects its
-- int day count ("IllegalStateException: Not an instance of
-- java.lang.Integer: 2026-09-07"), which fails the tiering job -- for every
-- table, since tiering runs with no restart strategy. ISO strings still
-- sort and compare correctly.
CREATE DATABASE IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.customers (
  customer_id  INT NOT NULL,
  email        STRING,
  full_name    STRING,
  country      STRING,
  created_at   TIMESTAMP(6),
  updated_at   TIMESTAMP(6),
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');

CREATE TABLE IF NOT EXISTS silver.products (
  product_id    INT NOT NULL,
  sku           STRING,
  product_name  STRING,
  category_id   INT,
  category_name STRING,
  price_cents   BIGINT,
  price         DECIMAL(12, 2),
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');

CREATE TABLE IF NOT EXISTS silver.orders (
  order_id          INT NOT NULL,
  customer_id       INT,
  status            STRING,
  order_total_cents BIGINT,
  order_total       DECIMAL(12, 2),
  order_date        STRING,  -- yyyy-MM-dd; see note at top of file
  created_at        TIMESTAMP(6),
  updated_at        TIMESTAMP(6),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');

CREATE TABLE IF NOT EXISTS silver.order_lines (
  order_item_id    INT NOT NULL,
  order_id         INT,
  customer_id      INT,
  order_status     STRING,
  order_date       STRING,  -- yyyy-MM-dd; see note at top of file
  product_id       INT,
  sku              STRING,
  product_name     STRING,
  category_id      INT,
  category_name    STRING,
  quantity         INT,
  unit_price_cents BIGINT,
  line_total_cents BIGINT,
  PRIMARY KEY (order_item_id) NOT ENFORCED
) WITH ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');

CREATE TABLE IF NOT EXISTS silver.payments (
  payment_id   INT NOT NULL,
  order_id     INT,
  amount_cents BIGINT,
  amount       DECIMAL(12, 2),
  `method`     STRING,  -- METHOD is a reserved word in Flink SQL
  status       STRING,
  processed_at TIMESTAMP(6),
  PRIMARY KEY (payment_id) NOT ENFORCED
) WITH ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');
