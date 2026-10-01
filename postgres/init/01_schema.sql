-- Same e-commerce schema as stream-debezium-kafka and stream-cdc-peerdb, with
-- one deliberate divergence: TIMESTAMPTZ columns became plain TIMESTAMP.
--
-- Why: Postgres TIMESTAMPTZ maps to Flink CDC's ZonedTimestampType, and
-- Fluss's sink connector doesn't support that type at all ("Unsupported
-- data type in fluss TIMESTAMP(6) WITH TIME ZONE"). The documented
-- workaround -- CAST(col AS TIMESTAMP) in a transform rule -- doesn't
-- actually work either: it hits a confirmed upstream bug in Flink CDC
-- 3.6.0's transform module (NumberFormatException in
-- BinaryRecordData.getZonedTimestamp, thrown *before* the cast expression
-- ever runs, while pre-transform is filling in the row's other fields).
-- See docs/architecture.md for the full reproduction. Rather than working
-- around a broken third-party code path, the schema itself avoids
-- TIMESTAMPTZ. This is a real, load-bearing limitation of this specific
-- version combination (Flink CDC 3.6.0 + Fluss 0.9.1's pipeline connector),
-- not a design preference -- worth re-testing on a newer Flink CDC release.
--
-- Flink CDC's Postgres source uses logical replication (same mechanism as
-- Debezium), so every table needs an explicit primary key for UPDATE/DELETE
-- events to carry enough of the old row to identify what changed.

CREATE TABLE categories (
    category_id     SERIAL PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE customers (
    customer_id      SERIAL PRIMARY KEY,
    email            TEXT NOT NULL UNIQUE,
    first_name       TEXT NOT NULL,
    last_name        TEXT NOT NULL,
    country          TEXT NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE products (
    product_id       SERIAL PRIMARY KEY,
    sku              TEXT NOT NULL UNIQUE,
    name             TEXT NOT NULL,
    category_id      INTEGER NOT NULL REFERENCES categories(category_id),
    price_cents      INTEGER NOT NULL CHECK (price_cents >= 0),
    description      TEXT,
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE orders (
    order_id         SERIAL PRIMARY KEY,
    customer_id      INTEGER NOT NULL REFERENCES customers(customer_id),
    status           TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    order_total_cents INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE order_items (
    order_item_id    SERIAL PRIMARY KEY,
    order_id         INTEGER NOT NULL REFERENCES orders(order_id),
    product_id       INTEGER NOT NULL REFERENCES products(product_id),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE payments (
    payment_id       SERIAL PRIMARY KEY,
    order_id         INTEGER NOT NULL REFERENCES orders(order_id),
    amount_cents     INTEGER NOT NULL CHECK (amount_cents >= 0),
    method           TEXT NOT NULL CHECK (method IN ('card', 'paypal', 'bank_transfer')),
    status           TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'succeeded', 'failed', 'refunded')),
    processed_at     TIMESTAMP
);

CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_payments_order ON payments(order_id);

-- Flink CDC's Postgres source (unlike Debezium/Kafka Connect in the sibling
-- project) creates its own publication automatically when none is given, so
-- no explicit CREATE PUBLICATION is required here. We still name tables
-- explicitly in the pipeline YAML rather than using a wildcard, for the same
-- "no silent surprises" discipline as the sibling repos.

-- REPLICA IDENTITY FULL is required, not optional, here -- confirmed by
-- testing. With Postgres's default REPLICA IDENTITY (DEFAULT), UPDATE/DELETE
-- WAL records only carry the primary key in the "before" image; every other
-- column comes through as NULL. Flink CDC 3.6.0's schema-type-inference code
-- (DebeziumSchemaDataTypeInference.inferStruct) NPEs when it tries to infer
-- a type from those null non-PK fields, which crash-loops the whole pipeline
-- job on the very first UPDATE or DELETE. FULL makes Postgres log the
-- complete old row instead, which is what the sibling repos' Debezium
-- connector also needs for the same reason (Debezium has no equivalent
-- crash, but would also lose old-row detail without FULL).
ALTER TABLE categories   REPLICA IDENTITY FULL;
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE products     REPLICA IDENTITY FULL;
ALTER TABLE orders       REPLICA IDENTITY FULL;
ALTER TABLE order_items  REPLICA IDENTITY FULL;
ALTER TABLE payments     REPLICA IDENTITY FULL;
