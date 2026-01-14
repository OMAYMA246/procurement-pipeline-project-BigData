-- Création de la table pour tous les stocks
-- Utilise le schéma default (déjà créé par le script orders)

DROP TABLE IF EXISTS hive.default.raw_stock;

CREATE TABLE hive.default.raw_stock (
    warehouse_id VARCHAR,
    warehouse_code VARCHAR,
    product_id INTEGER,
    sku VARCHAR,
    available_quantity INTEGER,
    reserved_quantity INTEGER,
    snapshot_date DATE
)
WITH (
    format = 'JSON',
    external_location = 'hdfs://namenode:9000/data/raw/stock/'
);

-- Vérification
SELECT 
    'Total Stock Records' as metric,
    CAST(COUNT(*) AS VARCHAR) as value
FROM hive.default.raw_stock
UNION ALL
SELECT 
    'Date Range',
    CAST(MIN(snapshot_date) AS VARCHAR) || ' to ' || CAST(MAX(snapshot_date) AS VARCHAR)
FROM hive.default.raw_stock
UNION ALL
SELECT 
    'Distinct Warehouses',
    CAST(COUNT(DISTINCT warehouse_id) AS VARCHAR)
FROM hive.default.raw_stock
UNION ALL
SELECT 
    'Distinct SKUs',
    CAST(COUNT(DISTINCT sku) AS VARCHAR)
FROM hive.default.raw_stock;

-- Vue agrégée par SKU et date (tous entrepôts combinés)
DROP VIEW IF EXISTS hive.default.v_stock_daily;

CREATE VIEW hive.default.v_stock_daily AS
SELECT 
    sku,
    snapshot_date,
    SUM(available_quantity) as total_available,
    SUM(reserved_quantity) as total_reserved,
    SUM(available_quantity - reserved_quantity) as net_available,
    COUNT(DISTINCT warehouse_id) as num_warehouses
FROM hive.default.raw_stock
GROUP BY sku, snapshot_date;

-- Aperçu de la vue
SELECT * FROM hive.default.v_stock_daily 
ORDER BY snapshot_date DESC, total_available DESC 
LIMIT 10;