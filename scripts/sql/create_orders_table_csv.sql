CREATE TABLE IF NOT EXISTS hive.procurement.orders (
    order_id VARCHAR,
    store_id VARCHAR,
    order_date VARCHAR,
    order_time VARCHAR,
    sku VARCHAR,
    product_name VARCHAR,
    quantity VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:9000/procurement/raw/orders',
    format = 'TEXTFILE',
    textfile_field_separator = ','
);