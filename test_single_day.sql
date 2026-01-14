-- Approche simple : Lire et parser les JSON directement avec Trino
-- Syntaxe UNNEST corrigée

CREATE SCHEMA IF NOT EXISTS hive.default;

-- Table temporaire pour lire le JSON brut d'un seul jour
DROP TABLE IF EXISTS hive.default.orders_2026_01_14;

CREATE TABLE hive.default.orders_2026_01_14 (
    json_content VARCHAR
)
WITH (
    format = 'TEXTFILE',
    external_location = 'hdfs://namenode:9000/data/raw/orders/2026-01-14/'
);

-- Voir ce qui est lu
SELECT 'Lignes totales' as info, CAST(COUNT(*) AS VARCHAR) as value
FROM hive.default.orders_2026_01_14;

-- Parser et extraire les commandes
DROP VIEW IF EXISTS hive.default.v_orders_2026_01_14;

CREATE VIEW hive.default.v_orders_2026_01_14 AS
WITH parsed_data AS (
    SELECT 
        CAST(json_parse(
            array_join(array_agg(json_content), '')
        ) AS ARRAY(ROW(
            order_id VARCHAR,
            store_id VARCHAR,
            order_date VARCHAR,
            order_time VARCHAR,
            customer_id VARCHAR,
            items ARRAY(ROW(
                sku VARCHAR,
                product_name VARCHAR,
                quantity INTEGER
            ))
        ))) as orders_array
    FROM hive.default.orders_2026_01_14
)
SELECT 
    order_id,
    CAST(order_date AS DATE) as order_date,
    store_id,
    customer_id,
    sku,
    product_name,
    quantity
FROM parsed_data
CROSS JOIN UNNEST(orders_array) AS t(order_id, store_id, order_date, order_time, customer_id, items)
CROSS JOIN UNNEST(items) AS item_t(sku, product_name, quantity);

-- Tester la vue
SELECT 
    'Total items vendus' as metric,
    CAST(SUM(quantity) AS VARCHAR) as value
FROM hive.default.v_orders_2026_01_14;

SELECT 
    sku,
    product_name,
    SUM(quantity) as total_quantity
FROM hive.default.v_orders_2026_01_14
GROUP BY sku, product_name
ORDER BY total_quantity DESC
LIMIT 10;

-- Même chose pour le stock
DROP TABLE IF EXISTS hive.default.stock_2026_01_14;

CREATE TABLE hive.default.stock_2026_01_14 (
    json_content VARCHAR
)
WITH (
    format = 'TEXTFILE',
    external_location = 'hdfs://namenode:9000/data/raw/stock/2026-01-14/'
);

DROP VIEW IF EXISTS hive.default.v_stock_2026_01_14;

CREATE VIEW hive.default.v_stock_2026_01_14 AS
WITH parsed_data AS (
    SELECT 
        CAST(json_parse(
            array_join(array_agg(json_content), '')
        ) AS ARRAY(ROW(
            warehouse_code VARCHAR,
            sku VARCHAR,
            product_name VARCHAR,
            available_quantity INTEGER,
            reserved_quantity INTEGER,
            snapshot_date VARCHAR,
            snapshot_time VARCHAR
        ))) as stock_array
    FROM hive.default.stock_2026_01_14
)
SELECT 
    warehouse_code,
    sku,
    product_name,
    available_quantity,
    reserved_quantity,
    CAST(snapshot_date AS DATE) as snapshot_date
FROM parsed_data
CROSS JOIN UNNEST(stock_array) AS t(warehouse_code, sku, product_name, available_quantity, reserved_quantity, snapshot_date, snapshot_time);

-- Tester
SELECT 
    'Total SKUs en stock' as metric,
    CAST(COUNT(DISTINCT sku) AS VARCHAR) as value
FROM hive.default.v_stock_2026_01_14;

SELECT 
    sku,
    product_name,
    SUM(available_quantity) as total_available
FROM hive.default.v_stock_2026_01_14
GROUP BY sku, product_name
ORDER BY total_available DESC
LIMIT 10;