SELECT 
    p.supplier_id,
    s.supplier_name,
    ord.sku,
    p.product_name,
    p.case_size,
    p.min_order_quantity,
    SUM(CAST(ord.quantity AS INTEGER)) as total_orders,
    MAX(CAST(st.available_quantity AS INTEGER)) as available_stock,
    MAX(CAST(st.reserved_quantity AS INTEGER)) as reserved_stock,
    p.safety_stock,
    GREATEST(0, 
        SUM(CAST(ord.quantity AS INTEGER)) + 
        p.safety_stock - 
        (MAX(CAST(st.available_quantity AS INTEGER)) - MAX(CAST(st.reserved_quantity AS INTEGER)))
    ) as net_demand
FROM hive.procurement.orders ord
LEFT JOIN hive.procurement.stock st ON ord.sku = st.sku
LEFT JOIN postgresql.public.products p ON ord.sku = p.sku
LEFT JOIN postgresql.public.suppliers s ON p.supplier_id = s.supplier_id
WHERE p.sku IS NOT NULL
GROUP BY p.supplier_id, s.supplier_name, ord.sku, p.product_name, p.case_size, p.min_order_quantity, p.safety_stock
HAVING GREATEST(0, 
        SUM(CAST(ord.quantity AS INTEGER)) + 
        p.safety_stock - 
        (MAX(CAST(st.available_quantity AS INTEGER)) - MAX(CAST(st.reserved_quantity AS INTEGER)))
    ) > 0
ORDER BY p.supplier_id, net_demand DESC;