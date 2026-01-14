-- Création de la table pour toutes les commandes
-- UNNEST corrigé avec tous les alias de colonnes

CREATE SCHEMA IF NOT EXISTS hive.default;

DROP TABLE IF EXISTS hive.default.raw_orders;

CREATE TABLE hive.default.raw_orders (
    order_id VARCHAR,
    order_date DATE,
    store_id VARCHAR,
    customer_id VARCHAR,
    total_amount DECIMAL(10,2),
    items ARRAY(ROW(
        product_id INTEGER,
        sku VARCHAR,
        product_name VARCHAR,
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        subtotal DECIMAL(10,2)
    ))
)
WITH (
    format = 'JSON',
    external_location = 'hdfs://namenode:9000/data/raw/orders/'
);

-- Vérification
SELECT 
    'Total Orders' as metric, 
    CAST(COUNT(*) AS VARCHAR) as value 
FROM hive.default.raw_orders
UNION ALL
SELECT 
    'Date Range', 
    CAST(MIN(order_date) AS VARCHAR) || ' to ' || CAST(MAX(order_date) AS VARCHAR)
FROM hive.default.raw_orders
UNION ALL
SELECT 
    'Distinct Stores',
    CAST(COUNT(DISTINCT store_id) AS VARCHAR)
FROM hive.default.raw_orders
UNION ALL
SELECT 
    'Total Items Sold',
    CAST(SUM(quantity) AS VARCHAR)
FROM hive.default.raw_orders
CROSS JOIN UNNEST(items) AS t(product_id, sku, product_name, quantity, unit_price, subtotal);

-- Vue agrégée par SKU et date
DROP VIEW IF EXISTS hive.default.v_orders_daily;

CREATE VIEW hive.default.v_orders_daily AS
SELECT 
    sku,
    product_name,
    o.order_date,
    SUM(quantity) as daily_quantity,
    SUM(subtotal) as daily_sales,
    COUNT(DISTINCT o.order_id) as num_orders,
    COUNT(DISTINCT o.store_id) as num_stores
FROM hive.default.raw_orders o
CROSS JOIN UNNEST(o.items) AS t(product_id, sku, product_name, quantity, unit_price, subtotal)
GROUP BY sku, product_name, o.order_date;

-- Aperçu de la vue
SELECT * FROM hive.default.v_orders_daily 
ORDER BY order_date DESC, daily_quantity DESC 
LIMIT 10;