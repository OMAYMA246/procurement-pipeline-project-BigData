-- Créer une table pour les items de commandes (format aplati)
CREATE TABLE IF NOT EXISTS hive.procurement.order_items (
    order_id VARCHAR,
    store_id VARCHAR,
    order_date VARCHAR,
    order_time VARCHAR,
    customer_id VARCHAR,
    sku VARCHAR,
    product_name VARCHAR,
    quantity INTEGER
)
WITH (
    format = 'PARQUET'
);

-- Créer une table pour les stocks
CREATE TABLE IF NOT EXISTS hive.procurement.stock_snapshots (
    warehouse_id VARCHAR,
    snapshot_date VARCHAR,
    sku VARCHAR,
    available_stock INTEGER,
    reserved_stock INTEGER,
    safety_stock INTEGER
)
WITH (
    format = 'PARQUET'
);