-- Test simple avec une table CSV
CREATE TABLE IF NOT EXISTS hive.procurement.test_orders (
    order_id VARCHAR,
    store_id VARCHAR,
    order_date VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:9000/procurement/raw/orders/2026-01-06',
    format = 'CSV',
    skip_header_line_count = 1
);