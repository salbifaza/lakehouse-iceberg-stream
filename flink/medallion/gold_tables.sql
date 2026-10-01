-- Gold layer tables. Run by run-medallion.sh only when the `gold`
-- database has none of them yet: with table.datalake.enabled, Fluss 0.9.1
-- rejects CREATE TABLE IF NOT EXISTS once the tiered Iceberg table exists
-- ("Table ... already exists"), so these DDLs are not safely re-runnable.
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_revenue_by_category (
  order_date    STRING NOT NULL,  -- yyyy-MM-dd, not DATE: see silver_tables.sql
  category_id   INT NOT NULL,
  category_name STRING,
  order_count   BIGINT,
  units_sold    BIGINT,
  revenue_cents BIGINT,
  revenue       DECIMAL(18, 2),
  PRIMARY KEY (order_date, category_id) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s',
  -- Fluss's Iceberg tiering supports only a single bucket key, and a PK
  -- table's bucket key defaults to the whole (composite) primary key
  -- ("Only one bucket key is supported for Iceberg at the moment").
  'bucket.key' = 'category_id'
);

CREATE TABLE IF NOT EXISTS gold.customer_lifetime_value (
  customer_id          INT NOT NULL,
  email                STRING,
  country              STRING,
  order_count          BIGINT,
  lifetime_value_cents BIGINT,
  lifetime_value       DECIMAL(18, 2),
  first_order_at       TIMESTAMP(6),
  last_order_at        TIMESTAMP(6),
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');

CREATE TABLE IF NOT EXISTS gold.order_status_summary (
  status            STRING NOT NULL,
  order_count       BIGINT,
  total_value_cents BIGINT,
  total_value       DECIMAL(18, 2),
  PRIMARY KEY (status) NOT ENFORCED
) WITH ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');
