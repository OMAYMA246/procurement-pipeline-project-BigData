-- Calcul du Net Demand selon la formule du projet
-- net_demand = max(0, aggregated_orders + safety_stock - (available_stock - reserved_stock))

SELECT 
    o.sku,
    p.product_name,
    p.supplier_id,
    SUM(CAST(o.quantity AS INTEGER)) as total_orders,
    MAX(CAST(s.available_quantity AS INTEGER)) as available_stock,
    MAX(CAST(s.reserved_quantity AS INTEGER)) as reserved_stock,
    p.safety_stock,
    GREATEST(0, 
        SUM(CAST(o.quantity AS INTEGER)) + 
        p.safety_stock - 
        (MAX(CAST(s.available_quantity AS INTEGER)) - MAX(CAST(s.reserved_quantity AS INTEGER)))
    ) as net_demand
FROM hive.procurement.orders o
LEFT JOIN hive.procurement.stock s ON o.sku = s.sku
LEFT JOIN postgresql.public.products p ON o.sku = p.sku
WHERE p.sku IS NOT NULL
GROUP BY o.sku, p.product_name, p.supplier_id, p.safety_stock
HAVING GREATEST(0, 
        SUM(CAST(o.quantity AS INTEGER)) + 
        p.safety_stock - 
        (MAX(CAST(s.available_quantity AS INTEGER)) - MAX(CAST(s.reserved_quantity AS INTEGER)))
    ) > 0
ORDER BY net_demand DESC
LIMIT 20;