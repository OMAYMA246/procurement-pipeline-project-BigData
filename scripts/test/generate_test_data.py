"""
Script pour générer des données de test qui forcent la création de commandes
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import os

def create_test_scenario(target_date):
    """Crée un scénario de test qui génère des commandes"""
    
    print(f"=== Création de scénario de test pour {target_date} ===\n")
    
    target_date_str = str(target_date)
    
    # 1. Réduire le stock disponible pour générer des commandes
    stock_path = Path(f"data/raw/stock/{target_date_str}")
    if stock_path.exists():
        print(f"📦 Réduction des stocks disponibles...")
        
        for csv_file in stock_path.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file)
                # Réduire le stock à 10% de sa valeur initiale
                df['available_quantity'] = df['available_quantity'].apply(
                    lambda x: max(1, int(x * 0.1))  # Garder au moins 1 unité
                )
                # Augmenter légèrement le stock réservé
                df['reserved_quantity'] = df['reserved_quantity'].apply(
                    lambda x: int(x * 1.5)
                )
                df.to_csv(csv_file, index=False)
                print(f"   ✓ {csv_file.name}: stock réduit à 10%")
            except Exception as e:
                print(f"   ⚠ Erreur sur {csv_file.name}: {e}")
    else:
        print(f"⚠️  Dossier stock non trouvé: {stock_path}")
    
    # 2. Augmenter la demande dans les commandes
    orders_path = Path(f"data/raw/orders/{target_date_str}")
    if orders_path.exists():
        print(f"\n🛒 Augmentation de la demande...")
        
        for csv_file in orders_path.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file)
                # Augmenter les quantités de 50%
                df['quantity'] = df['quantity'].apply(
                    lambda x: int(x * 1.5)
                )
                df.to_csv(csv_file, index=False)
                print(f"   ✓ {csv_file.name}: demande augmentée de 50%")
            except Exception as e:
                print(f"   ⚠ Erreur sur {csv_file.name}: {e}")
    else:
        print(f"⚠️  Dossier orders non trouvé: {orders_path}")
    
    # 3. Nettoyer les fichiers agrégés existants pour forcer un recalcul
    agg_file = Path(f"data/processed/aggregated_orders/aggregated_orders_{target_date_str}.csv")
    if agg_file.exists():
        agg_file.unlink()
        print(f"\n🗑️  Fichier agrégé supprimé: {agg_file}")
    
    net_demand_file = Path(f"data/processed/net_demand/net_demand_{target_date_str}.csv")
    if net_demand_file.exists():
        net_demand_file.unlink()
        print(f"🗑️  Fichier net demand supprimé: {net_demand_file}")
    
    # 4. Nettoyer les commandes fournisseurs existantes
    supplier_orders_dir = Path(f"data/output/supplier_orders/{target_date_str}")
    if supplier_orders_dir.exists():
        for json_file in supplier_orders_dir.glob("*.json"):
            json_file.unlink()
        print(f"🗑️  Commandes fournisseurs supprimées: {supplier_orders_dir}")
    
    print(f"\n✅ Scénario de test créé pour {target_date_str}")
    print("   • Stocks réduits à 10%")
    print("   • Demande augmentée de 50%")
    print("   • Fichiers calculés supprimés")
    print(f"\n⚠️  Le pipeline devrait maintenant générer des commandes pour {target_date_str}")

def create_complete_test_dataset():
    """Crée un jeu de données de test complet"""
    
    print("=== Création de jeu de données de test complet ===\n")
    
    # Date de test
    test_date = "2026-01-14"
    
    # 1. Créer des données produits
    products_data = []
    for i in range(1, 101):
        products_data.append({
            'sku': f'SKU{i:05d}',
            'product_name': f'Produit Test {i}',
            'supplier_id': f'SUP{(i % 5) + 1:03d}',  # 5 fournisseurs
            'pack_size': [1, 2, 3, 6, 12, 24, 48][i % 7],
            'min_order_quantity': [10, 20, 30, 40, 50][i % 5],
            'safety_stock': [5, 10, 15, 20, 25][i % 5]
        })
    
    products_df = pd.DataFrame(products_data)
    products_path = Path('data/master/products.csv')
    products_path.parent.mkdir(parents=True, exist_ok=True)
    products_df.to_csv(products_path, index=False)
    print(f"✓ Fichier produits créé: {products_path}")
    
    # 2. Créer des données de commandes
    orders_path = Path(f'data/raw/orders/{test_date}')
    orders_path.mkdir(parents=True, exist_ok=True)
    
    # Créer 5 fichiers de commandes (simulant 5 magasins)
    for store_id in range(1, 6):
        store_orders = []
        for i in range(100):  # 100 lignes par magasin
            sku_idx = np.random.randint(0, 100)
            store_orders.append({
                'sku': f'SKU{sku_idx + 1:05d}',
                'product_name': f'Produit Test {sku_idx + 1}',
                'quantity': np.random.randint(1, 50),
                'store_id': f'STORE{store_id:03d}',
                'customer_id': f'CUST{np.random.randint(1000, 9999)}'
            })
        
        store_df = pd.DataFrame(store_orders)
        store_file = orders_path / f'orders_store_{store_id}_{test_date}.csv'
        store_df.to_csv(store_file, index=False)
        print(f"✓ Commandes magasin {store_id}: {store_file}")
    
    # 3. Créer des données de stock (stock bas pour générer des commandes)
    stock_path = Path(f'data/raw/stock/{test_date}')
    stock_path.mkdir(parents=True, exist_ok=True)
    
    stock_data = []
    for i in range(1, 101):
        stock_data.append({
            'sku': f'SKU{i:05d}',
            'warehouse_id': 'WH001',
            'available_quantity': np.random.randint(1, 20),  # Stock bas
            'reserved_quantity': np.random.randint(0, 5),
            'location': f'LOC{i:03d}'
        })
    
    stock_df = pd.DataFrame(stock_data)
    stock_file = stock_path / f'stock_{test_date}.csv'
    stock_df.to_csv(stock_file, index=False)
    print(f"✓ Données stock créées: {stock_file}")
    
    print(f"\n✅ Jeu de données de test complet créé pour {test_date}")
    print(f"   • 100 produits avec 5 fournisseurs")
    print(f"   • 5 fichiers de commandes (500 lignes total)")
    print(f"   • Stock bas pour générer des commandes")
    print(f"\nExécutez: python scripts/orchestration/run_procurement_pipeline.py --date {test_date}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Générateur de données de test')
    parser.add_argument('--date', type=str, default=None,
                       help='Date pour le scénario de test (format: YYYY-MM-DD)')
    parser.add_argument('--complete', action='store_true',
                       help='Créer un jeu de données complet')
    
    args = parser.parse_args()
    
    if args.complete:
        create_complete_test_dataset()
    else:
        target_date = args.date or datetime.now().date()
        create_test_scenario(target_date)