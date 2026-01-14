"""
Script pour convertir les fichiers JSON array en JSON Lines
Format actuel: [{...}, {...}]
Format cible: {...}\n{...}\n (une ligne par objet)
"""

import json
import os
import subprocess
from pathlib import Path

# Configuration
DATES = ['2026-01-08', '2026-01-09', '2026-01-10', '2026-01-11', 
         '2026-01-12', '2026-01-13', '2026-01-14']
STORES = ['STORE01', 'STORE02', 'STORE03', 'STORE04', 'STORE05']
WAREHOUSES = ['WH01', 'WH02', 'WH03']

TEMP_LOCAL = '/tmp/jsonlines'
HDFS_ORDERS = '/data/raw/orders_jsonlines'
HDFS_STOCK = '/data/raw/stock_jsonlines'

def download_from_hdfs(hdfs_path, local_path):
    """Télécharge un fichier depuis HDFS"""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    cmd = ['hdfs', 'dfs', '-get', hdfs_path, local_path]
    result = subprocess.run(cmd, capture_output=True)
    return result.returncode == 0

def upload_to_hdfs(local_path, hdfs_path):
    """Upload un fichier vers HDFS"""
    # Créer le répertoire parent
    parent = os.path.dirname(hdfs_path)
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', parent], 
                  capture_output=True)
    
    # Upload
    cmd = ['hdfs', 'dfs', '-put', '-f', local_path, hdfs_path]
    result = subprocess.run(cmd, capture_output=True)
    return result.returncode == 0

def convert_json_to_jsonlines(input_file, output_file):
    """Convertit un fichier JSON array en JSON Lines"""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print(f"  ⚠ Pas un tableau JSON: {input_file}")
            return False
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        return True
    except Exception as e:
        print(f"  ✗ Erreur: {e}")
        return False

def process_orders():
    """Traite tous les fichiers de commandes"""
    print("\n=== CONVERSION DES COMMANDES ===")
    
    total_converted = 0
    
    for date in DATES:
        print(f"\n--- Date: {date} ---")
        
        for store_id in STORES:
            filename = f"orders_{store_id.lower()}.json"
            
            # Chemins
            hdfs_src = f"/data/raw/orders/{date}/{filename}"
            local_src = f"{TEMP_LOCAL}/orders/{date}/{filename}"
            local_dst = f"{TEMP_LOCAL}/orders_jsonlines/{date}/{filename}"
            hdfs_dst = f"{HDFS_ORDERS}/{date}/{filename}"
            
            print(f"  {store_id}...", end=' ')
            
            # Télécharger depuis HDFS
            if not download_from_hdfs(hdfs_src, local_src):
                print("✗ Download failed")
                continue
            
            # Convertir
            if not convert_json_to_jsonlines(local_src, local_dst):
                print("✗ Conversion failed")
                continue
            
            # Upload vers HDFS
            if not upload_to_hdfs(local_dst, hdfs_dst):
                print("✗ Upload failed")
                continue
            
            print("✓")
            total_converted += 1
    
    print(f"\n✓ {total_converted} fichiers de commandes convertis")

def process_stock():
    """Traite tous les fichiers de stock"""
    print("\n=== CONVERSION DU STOCK ===")
    
    total_converted = 0
    
    for date in DATES:
        print(f"\n--- Date: {date} ---")
        
        for warehouse in WAREHOUSES:
            filename = f"stock_{warehouse}.json"
            
            # Chemins
            hdfs_src = f"/data/raw/stock/{date}/{filename}"
            local_src = f"{TEMP_LOCAL}/stock/{date}/{filename}"
            local_dst = f"{TEMP_LOCAL}/stock_jsonlines/{date}/{filename}"
            hdfs_dst = f"{HDFS_STOCK}/{date}/{filename}"
            
            print(f"  {warehouse}...", end=' ')
            
            # Télécharger depuis HDFS
            if not download_from_hdfs(hdfs_src, local_src):
                print("✗ Download failed")
                continue
            
            # Convertir
            if not convert_json_to_jsonlines(local_src, local_dst):
                print("✗ Conversion failed")
                continue
            
            # Upload vers HDFS
            if not upload_to_hdfs(local_dst, hdfs_dst):
                print("✗ Upload failed")
                continue
            
            print("✓")
            total_converted += 1
    
    print(f"\n✓ {total_converted} fichiers de stock convertis")

def main():
    print("="*70)
    print("CONVERSION JSON ARRAY → JSON LINES")
    print("="*70)
    
    # Nettoyer le dossier temporaire
    os.makedirs(TEMP_LOCAL, exist_ok=True)
    
    # Convertir les commandes
    process_orders()
    
    # Convertir le stock
    process_stock()
    
    print("\n" + "="*70)
    print("✓ CONVERSION TERMINÉE")
    print("="*70)
    print(f"\nNouvelles données disponibles dans HDFS:")
    print(f"  - Orders: {HDFS_ORDERS}/")
    print(f"  - Stock: {HDFS_STOCK}/")
    print(f"\nMaintenant, mettez à jour vos tables Trino pour pointer vers:")
    print(f"  external_location = 'hdfs://namenode:9000{HDFS_ORDERS}/'")
    print(f"  external_location = 'hdfs://namenode:9000{HDFS_STOCK}/'")

if __name__ == "__main__":
    main()