-- Tables adaptées au format JSON réel (tableaux JSON)
-- Syntaxe UNNEST corrigée pour Trino

CREATE SCHEMA IF NOT EXISTS hive.default;

-- ==================== ORDERS ====================

-- Étape 1: Lire le JSON brut
DROP TABLE IF EXISTS hive.default.raw_orders_json;

CREATE TABLE hive.default.raw_orders_json (
    json_data VARCHAR
)
WITH (
    format = 'TEXTFILE',
    external_location = 'hdfs://namenode:9000/data/raw/orders/'
);

-- Étape 2: Vue pour parser le JSON
DROP VIEW IF EXISTS hive.default.v_orders_parsed;

CREATE VIEW hive.default.v_orders_parsed AS
SELECT 
    order_id,
    CAST(order_date AS DATE) as order_date,
    store_id,
    customer_id,
    items
FROM hive.default.raw_orders_json,
UNNEST(
    CAST(json_parse(json_data) AS ARRAY(ROW(
        order_id VARCHAR,
        order_date VARCHAR,
        store_id VARCHAR,
        customer_id VARCHAR,
        order_time VARCHAR,
        items ARRAY(ROW(
            sku VARCHAR,
            product_name VARCHAR,
            quantity INTEGER
        ))
    )))
) AS t(order_id, order_date, store_id, customer_id, order_time, items);

-- Étape 3: Vue finale avec items dénormalisés
DROP VIEW IF EXISTS hive.default.v_orders_daily;

CREATE VIEW hive.default.v_orders_daily AS
SELECT 
    sku,
    product_name,
    o.order_date,
    SUM(quantity) as daily_quantity,
    COUNT(DISTINCT o.order_id) as num_orders,
    COUNT(DISTINCT o.store_id) as num_stores
FROM hive.default.v_orders_parsed o
CROSS JOIN UNNEST(o.items) AS t(sku, product_name, quantity)
GROUP BY sku, product_name, o.order_date;

-- Vérifications
SELECT 'Total Orders' as metric, CAST(COUNT(*) AS VARCHAR) as value
FROM hive.default.v_orders_parsed
UNION ALL
SELECT 'Date Range', 
    CAST(MIN(order_date) AS VARCHAR) || ' to ' || CAST(MAX(order_date) AS VARCHAR)
FROM hive.default.v_orders_parsed;

SELECT * FROM hive.default.v_orders_daily
ORDER BY order_date DESC, daily_quantity DESC
LIMIT 5;

-- ==================== STOCK ====================

-- Étape 1: Lire le JSON brut
DROP TABLE IF EXISTS hive.default.raw_stock_json;

CREATE TABLE hive.default.raw_stock_json (
    json_data VARCHAR
)
WITH (
    format = 'TEXTFILE',
    external_location = 'hdfs://namenode:9000/data/raw/stock/'
);

-- Étape 2: Vue pour parser le JSON
DROP VIEW IF EXISTS hive.default.v_stock_parsed;

CREATE VIEW hive.default.v_stock_parsed AS
SELECT 
    warehouse_code,
    sku,
    product_name,
    available_quantity,
    reserved_quantity,
    CAST(snapshot_date AS DATE) as snapshot_date
FROM hive.default.raw_stock_json,
UNNEST(
    CAST(json_parse(json_data) AS ARRAY(ROW(
        warehouse_code VARCHAR,
        sku VARCHAR,
        product_name VARCHAR,
        available_quantity INTEGER,
        reserved_quantity INTEGER,
        snapshot_date VARCHAR,
        snapshot_time VARCHAR
    )))
) AS t(warehouse_code, sku, product_name, available_quantity, reserved_quantity, snapshot_date, snapshot_time);

-- Étape 3: Vue agrégée par SKU et date
DROP VIEW IF EXISTS hive.default.v_stock_daily;

CREATE VIEW hive.default.v_stock_daily AS
SELECT 
    sku,
    product_name,
    snapshot_date,
    SUM(available_quantity) as total_available,
    SUM(reserved_quantity) as total_reserved,
    SUM(available_quantity - reserved_quantity) as net_available,
    COUNT(DISTINCT warehouse_code) as num_warehouses
FROM hive.default.v_stock_parsed
GROUP BY sku, product_name, snapshot_date;

-- Vérifications
SELECT 'Total Stock Records' as metric, CAST(COUNT(*) AS VARCHAR) as value
FROM hive.default.v_stock_parsed
UNION ALL
SELECT 'Date Range',
    CAST(MIN(snapshot_date) AS VARCHAR) || ' to ' || CAST(MAX(snapshot_date) AS VARCHAR)
FROM hive.default.v_stock_parsed;

SELECT * FROM hive.default.v_stock_daily
ORDER BY snapshot_date DESC, total_available DESC
LIMIT 5;