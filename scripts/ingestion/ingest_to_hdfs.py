"""
Script d'ingestion des données vers HDFS
Transfère les fichiers depuis le système local vers HDFS
"""

import os
import subprocess
from datetime import datetime, timedelta
import yaml

# Chargement de la configuration
config_path = os.path.join(os.path.dirname(__file__), '../../config/config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

hdfs_config = config['hdfs']
paths_config = config['paths']
data_gen_config = config['data_generation']

# Configuration HDFS
hdfs_base_path = hdfs_config['base_path']

print("=== Ingestion des données vers HDFS ===\n")

def run_hdfs_command(command):
    """Exécute une commande HDFS directement"""
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True
        )
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

# 1. Création de la structure de répertoires HDFS
print("1. Création de la structure HDFS...")
hdfs_dirs = [
    f"{hdfs_base_path}/raw/orders",
    f"{hdfs_base_path}/raw/stock",
    f"{hdfs_base_path}/processed/aggregated_orders",
    f"{hdfs_base_path}/processed/net_demand",
    f"{hdfs_base_path}/output/supplier_orders",
    f"{hdfs_base_path}/logs/exceptions"
]

for hdfs_dir in hdfs_dirs:
    success, stdout, stderr = run_hdfs_command(f"hdfs dfs -mkdir -p {hdfs_dir}")
    if success:
        print(f"   ✓ {hdfs_dir}")
    else:
        print(f"   ✗ Erreur création {hdfs_dir}: {stderr}")

# 2. Transfert des fichiers orders vers HDFS
print("\n2. Transfert des fichiers de commandes vers HDFS...")
base_date = datetime.now().date()
date_range_days = data_gen_config['date_range_days']

for day_offset in range(date_range_days):
    current_date = base_date - timedelta(days=date_range_days - day_offset - 1)
    date_str = current_date.strftime('%Y-%m-%d')
    
    local_orders_path = f"/app/data/raw/orders/{date_str}"
    hdfs_orders_path = f"{hdfs_base_path}/raw/orders/"
    
    # Vérifier si le dossier local existe
    if os.path.exists(local_orders_path):
        # Copie vers HDFS
        success, stdout, stderr = run_hdfs_command(
            f"hdfs dfs -put -f {local_orders_path} {hdfs_orders_path}"
        )
        
        if success:
            print(f"   ✓ Commandes du {date_str} transférées")
        else:
            print(f"   ✗ Erreur pour {date_str}: {stderr}")
    else:
        print(f"   ⚠ Dossier local introuvable: {local_orders_path}")

# 3. Transfert des fichiers stock vers HDFS
print("\n3. Transfert des snapshots de stock vers HDFS...")
for day_offset in range(date_range_days):
    current_date = base_date - timedelta(days=date_range_days - day_offset - 1)
    date_str = current_date.strftime('%Y-%m-%d')
    
    local_stock_path = f"/app/data/raw/stock/{date_str}"
    hdfs_stock_path = f"{hdfs_base_path}/raw/stock/"
    
    # Vérifier si le dossier local existe
    if os.path.exists(local_stock_path):
        # Copie vers HDFS
        success, stdout, stderr = run_hdfs_command(
            f"hdfs dfs -put -f {local_stock_path} {hdfs_stock_path}"
        )
        
        if success:
            print(f"   ✓ Stock du {date_str} transféré")
        else:
            print(f"   ✗ Erreur pour {date_str}: {stderr}")
    else:
        print(f"   ⚠ Dossier local introuvable: {local_stock_path}")

# 4. Vérification des fichiers dans HDFS
print("\n4. Vérification des fichiers dans HDFS...")
print("\n   Structure des commandes:")
success, stdout, stderr = run_hdfs_command(f"hdfs dfs -ls {hdfs_base_path}/raw/orders")
if success:
    print(stdout)
else:
    print(f"   Erreur: {stderr}")

print("\n   Structure des stocks:")
success, stdout, stderr = run_hdfs_command(f"hdfs dfs -ls {hdfs_base_path}/raw/stock")
if success:
    print(stdout)
else:
    print(f"   Erreur: {stderr}")

# 5. Compter les fichiers transférés
print("\n5. Statistiques de transfert:")
success, stdout, stderr = run_hdfs_command(f"hdfs dfs -count {hdfs_base_path}/raw")
if success:
    print(stdout)

print("\n=== Ingestion terminée ! ===")