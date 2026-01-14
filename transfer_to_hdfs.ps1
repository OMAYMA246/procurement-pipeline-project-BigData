# Script de transfert des résultats vers HDFS
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  TRANSFERT DES RESULTATS VERS HDFS" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# 1. Transfert des commandes agregees
Write-Host "`n[1/3] Transfert des commandes agregees..." -ForegroundColor Yellow
docker cp data/processed/aggregated_orders procurement_namenode:/tmp/aggregated_orders
docker exec procurement_namenode hdfs dfs -rm -r /procurement/processed/aggregated_orders
docker exec procurement_namenode hdfs dfs -put /tmp/aggregated_orders /procurement/processed/
Write-Host "   OK Commandes agregees transferees" -ForegroundColor Green

# 2. Transfert du net demand
Write-Host "`n[2/3] Transfert du net demand..." -ForegroundColor Yellow
docker cp data/processed/net_demand procurement_namenode:/tmp/net_demand
docker exec procurement_namenode hdfs dfs -rm -r /procurement/processed/net_demand
docker exec procurement_namenode hdfs dfs -put /tmp/net_demand /procurement/processed/
Write-Host "   OK Net demand transfere" -ForegroundColor Green

# 3. Transfert des commandes fournisseurs
Write-Host "`n[3/3] Transfert des commandes fournisseurs..." -ForegroundColor Yellow
docker cp data/output/supplier_orders procurement_namenode:/tmp/supplier_orders
docker exec procurement_namenode hdfs dfs -rm -r /procurement/output/supplier_orders
docker exec procurement_namenode hdfs dfs -put /tmp/supplier_orders /procurement/output/
Write-Host "   OK Commandes fournisseurs transferees" -ForegroundColor Green

# 4. Verification
Write-Host "`n=============================================" -ForegroundColor Cyan
Write-Host "  VERIFICATION DU CONTENU HDFS" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

Write-Host "`nContenu global de /procurement:" -ForegroundColor Yellow
docker exec procurement_namenode hdfs dfs -du -h /procurement

Write-Host "`nFichiers agreges:" -ForegroundColor Yellow
docker exec procurement_namenode hdfs dfs -ls /procurement/processed/aggregated_orders

Write-Host "`nFichiers net demand:" -ForegroundColor Yellow
docker exec procurement_namenode hdfs dfs -ls /procurement/processed/net_demand

Write-Host "`nCommandes fournisseurs (par date):" -ForegroundColor Yellow
docker exec procurement_namenode hdfs dfs -ls /procurement/output/supplier_orders

Write-Host "`n=============================================" -ForegroundColor Green
Write-Host "  TRANSFERT TERMINE AVEC SUCCES !" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green