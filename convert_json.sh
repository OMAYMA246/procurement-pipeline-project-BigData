docker exec procurement_namenode bash -c "cat > /tmp/convert.sh" << 'ENDOFSCRIPT'
#!/bin/bash
echo "======================================================================"
echo "CONVERSION JSON ARRAY → JSON LINES"
echo "======================================================================"

DATES=("2026-01-08" "2026-01-09" "2026-01-10" "2026-01-11" "2026-01-12" "2026-01-13" "2026-01-14")
STORES=("store01" "store02" "store03" "store04" "store05")
WAREHOUSES=("WH01" "WH02" "WH03")

echo "Installation de jq..."
apk add --no-cache jq 2>/dev/null || echo "jq non disponible"

echo ""
echo "=== CONVERSION DES COMMANDES ==="

total=0
for date in "${DATES[@]}"; do
    echo ""
    echo "--- Date: $date ---"
    
    for store in "${STORES[@]}"; do
        filename="orders_${store}.json"
        hdfs_src="/data/raw/orders/${date}/${filename}"
        hdfs_dst="/data/raw/orders_jsonlines/${date}/${filename}"
        
        echo -n "  ${store}... "
        
        hdfs dfs -get "$hdfs_src" /tmp/temp.json 2>/dev/null
        if [ $? -ne 0 ]; then
            echo "✗"
            continue
        fi
        
        jq -c '.[]' /tmp/temp.json > /tmp/temp_converted.json 2>/dev/null
        
        hdfs dfs -mkdir -p "/data/raw/orders_jsonlines/${date}" 2>/dev/null
        hdfs dfs -put -f /tmp/temp_converted.json "$hdfs_dst" 2>/dev/null
        
        echo "✓"
        ((total++))
    done
done

echo ""
echo "✓ $total fichiers convertis"

rm -f /tmp/temp.json /tmp/temp_converted.json
ENDOFSCRIPT