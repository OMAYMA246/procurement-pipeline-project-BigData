"""
Script de génération des données opérationnelles
Génère: Orders (commandes clients) et Stock Snapshots
Format: JSON et CSV pour simulation POS
"""

import pandas as pd
from faker import Faker
import random
import json
import os
from datetime import datetime, timedelta
import psycopg2
import yaml

# Initialisation
fake = Faker('fr_FR')
Faker.seed(42)
random.seed(42)

# Chargement de la configuration
config_path = os.path.join(os.path.dirname(__file__), '../../config/config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

db_config = config['database']['postgresql']
data_gen_config = config['data_generation']
paths_config = config['paths']

# Connexion à PostgreSQL pour récupérer les données master
conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    database=db_config['database'],
    user=db_config['user'],
    password=db_config['password']
)

# Récupération des produits et entrepôts
products_df = pd.read_sql("SELECT product_id, sku, product_name FROM products", conn)
warehouses_df = pd.read_sql("SELECT warehouse_id, warehouse_code FROM warehouses", conn)
conn.close()

print("=== Génération des Données Opérationnelles ===\n")

# Configuration
num_stores = data_gen_config['num_stores']
date_range_days = data_gen_config['date_range_days']
base_date = datetime.now().date()

# Création des dossiers si nécessaire
os.makedirs('data/raw/orders', exist_ok=True)
os.makedirs('data/raw/stock', exist_ok=True)

# Génération pour chaque jour
for day_offset in range(date_range_days):
    current_date = base_date - timedelta(days=date_range_days - day_offset - 1)
    date_str = current_date.strftime('%Y-%m-%d')
    
    print(f"Génération des données pour {date_str}...")
    
    # Création des dossiers par date
    orders_dir = f'data/raw/orders/{date_str}'
    stock_dir = f'data/raw/stock/{date_str}'
    os.makedirs(orders_dir, exist_ok=True)
    os.makedirs(stock_dir, exist_ok=True)
    
    # 1. GÉNÉRATION DES COMMANDES (ORDERS) PAR STORE/POS
    print(f"  - Génération des commandes...")
    for store_id in range(1, num_stores + 1):
        orders = []
        
        # Nombre aléatoire de commandes par magasin (50-200)
        num_orders = random.randint(50, 200)
        
        for order_id in range(1, num_orders + 1):
            # Sélection aléatoire de 1-10 produits par commande
            num_items = random.randint(1, 10)
            selected_products = products_df.sample(n=num_items)
            
            order = {
                'order_id': f'ORD-{date_str}-S{store_id:02d}-{order_id:04d}',
                'store_id': f'STORE{store_id:02d}',
                'order_date': date_str,
                'order_time': f"{random.randint(8, 21)}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
                'customer_id': fake.uuid4(),
                'items': []
            }
            
            for _, product in selected_products.iterrows():
                item = {
                    'sku': product['sku'],
                    'product_name': product['product_name'],
                    'quantity': random.randint(1, 5)
                }
                order['items'].append(item)
            
            orders.append(order)
        
        # Sauvegarde au format JSON
        json_filename = f'{orders_dir}/orders_store_{store_id:02d}.json'
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(orders, f, ensure_ascii=False, indent=2)
        
        # Sauvegarde au format CSV (version aplatie)
        csv_data = []
        for order in orders:
            for item in order['items']:
                csv_data.append({
                    'order_id': order['order_id'],
                    'store_id': order['store_id'],
                    'order_date': order['order_date'],
                    'order_time': order['order_time'],
                    'sku': item['sku'],
                    'product_name': item['product_name'],
                    'quantity': item['quantity']
                })
        
        csv_filename = f'{orders_dir}/orders_store_{store_id:02d}.csv'
        pd.DataFrame(csv_data).to_csv(csv_filename, index=False)
    
    print(f"    ✓ {num_stores} fichiers de commandes créés (JSON + CSV)")
    
    # 2. GÉNÉRATION DES STOCK SNAPSHOTS PAR WAREHOUSE
    print(f"  - Génération des snapshots de stock...")
    for _, warehouse in warehouses_df.iterrows():
        stock_snapshot = []
        
        for _, product in products_df.iterrows():
            # Stock disponible : entre 0 et 500
            available = random.randint(0, 500)
            # Stock réservé : entre 0 et 20% du disponible
            reserved = random.randint(0, int(available * 0.2)) if available > 0 else 0
            
            stock_item = {
                'warehouse_code': warehouse['warehouse_code'],
                'sku': product['sku'],
                'product_name': product['product_name'],
                'available_quantity': available,
                'reserved_quantity': reserved,
                'snapshot_date': date_str,
                'snapshot_time': '23:59:59'
            }
            stock_snapshot.append(stock_item)
        
        # Sauvegarde JSON
        json_filename = f'{stock_dir}/stock_{warehouse["warehouse_code"]}.json'
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(stock_snapshot, f, ensure_ascii=False, indent=2)
        
        # Sauvegarde CSV
        csv_filename = f'{stock_dir}/stock_{warehouse["warehouse_code"]}.csv'
        pd.DataFrame(stock_snapshot).to_csv(csv_filename, index=False)
    
    print(f"    ✓ {len(warehouses_df)} fichiers de stock créés (JSON + CSV)")
    print()

print("=== Données opérationnelles générées avec succès ! ===")
print(f"Période: {date_range_days} jours")
print(f"Stores: {num_stores}")
print(f"Warehouses: {len(warehouses_df)}")
print(f"Produits: {len(products_df)}")