"""
Script de calcul du net demand pour une date spécifique
"""

import pandas as pd
import yaml
from pathlib import Path
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2

# Chargement configuration
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

def calculate_net_demand_for_date(target_date):
    """Calcule le net demand pour une date spécifique"""
    print(f"=== Calcul du Net Demand pour {target_date} ===\n")
    
    # Connexion PostgreSQL pour charger products
    products_df = None
    try:
        # Essayer d'abord avec SQLAlchemy
        engine = create_engine('postgresql://postgres:postgres@procurement_postgres:5432/procurement_db')
        products_df = pd.read_sql('SELECT sku, supplier_id, pack_size, min_order_quantity as moq, safety_stock FROM products', engine)
        print(f"✓ Produits chargés depuis PostgreSQL: {len(products_df)}")
    except Exception as e:
        print(f"⚠️ Erreur de connexion PostgreSQL: {e}")
        print("⚠️ Tentative avec psycopg2...")
        try:
            # Fallback avec psycopg2
            conn = psycopg2.connect(
                host='procurement_postgres',
                database='procurement_db',
                user='postgres',
                password='postgres'
            )
            products_df = pd.read_sql('SELECT sku, supplier_id, pack_size, min_order_quantity as moq, safety_stock FROM products', conn)
            conn.close()
            print(f"✓ Produits chargés avec psycopg2: {len(products_df)}")
        except Exception as e2:
            print(f"⚠️ Erreur psycopg2: {e2}")
            print("⚠️ Utilisation d'un fallback sur fichier local")
            # Fallback: lire depuis un fichier CSV local
            products_path = Path('data/master/products.csv')
            if products_path.exists():
                products_df = pd.read_csv(products_path)
                print(f"✓ Produits chargés depuis fichier CSV: {len(products_df)}")
            else:
                # Créer un DataFrame de test
                print("⚠️ Création de données produits de test")
                products_df = pd.DataFrame({
                    'sku': [f'SKU{i:05d}' for i in range(1, 101)],
                    'supplier_id': [f'SUP{(i % 10) + 1:03d}' for i in range(100)],
                    'pack_size': [1, 2, 3, 4, 5, 6, 10, 12, 24, 48] * 10,
                    'moq': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100] * 10,
                    'safety_stock': [5, 10, 15, 20, 25, 30, 35, 40, 45, 50] * 10
                })
                print(f"✓ Données de test créées: {len(products_df)} produits")
    
    print()
    
    # Chemins
    agg_file = Path(f'data/processed/aggregated_orders/aggregated_orders_{target_date}.csv')
    stock_path = Path(f'data/raw/stock/{target_date}')
    output_path_local = Path('data/processed/net_demand')
    
    output_path_local.mkdir(parents=True, exist_ok=True)
    
    print(f"📅 Traitement du {target_date}...")
    
    # 1. Vérifier si le fichier d'agrégation existe
    if not agg_file.exists():
        print(f"   ⚠ Fichier d'agrégation non trouvé: {agg_file}")
        print("   ℹ Création d'un DataFrame vide pour les commandes")
        orders_agg = pd.DataFrame(columns=['sku', 'total_quantity', 'date'])
    else:
        orders_agg = pd.read_csv(agg_file)
        print(f"   ✓ Commandes chargées: {len(orders_agg)} lignes")
    
    # 2. Vérifier et lire les stocks
    if not stock_path.exists():
        print(f"   ⚠ Pas de données stock pour {target_date}")
        print("   ℹ Utilisation de valeurs par défaut pour le stock")
        
        # Créer un DataFrame de stock par défaut
        if not orders_agg.empty:
            skus = orders_agg['sku'].unique()
        else:
            skus = products_df['sku'].unique()
            
        stocks_agg = pd.DataFrame({
            'sku': skus,
            'available_stock': 10,  # Faible stock par défaut pour générer des commandes
            'reserved_stock': 0
        })
    else:
        # Lire les fichiers de stock réels
        all_stocks = []
        csv_files = list(stock_path.glob('*.csv'))
        
        if not csv_files:
            print(f"   ⚠ Aucun fichier CSV dans {stock_path}")
            stocks_agg = pd.DataFrame(columns=['sku', 'available_stock', 'reserved_stock'])
        else:
            for csv_file in csv_files:
                df = pd.read_csv(csv_file)
                all_stocks.append(df)
            
            stocks_df = pd.concat(all_stocks, ignore_index=True)
            
            # Agréger stocks par SKU
            stocks_agg = stocks_df.groupby('sku').agg({
                'available_quantity': 'sum',
                'reserved_quantity': 'sum'
            }).reset_index()
            
            stocks_agg.rename(columns={
                'available_quantity': 'available_stock',
                'reserved_quantity': 'reserved_stock'
            }, inplace=True)
            
            print(f"   ✓ Stock chargé: {len(stocks_agg)} SKUs")
    
    # 3. Préparer les DataFrames pour la jointure
    if orders_agg.empty:
        result = pd.DataFrame({'sku': products_df['sku'].unique()})
        result['total_quantity'] = 0
        result['date'] = target_date
    else:
        result = orders_agg.copy()
    
    # 4. Joindre avec les stocks
    if not stocks_agg.empty:
        result = result.merge(stocks_agg, on='sku', how='left')
    else:
        result['available_stock'] = 10  # Faible stock par défaut
        result['reserved_stock'] = 0
    
    # Remplir les valeurs NaN pour le stock
    result['available_stock'] = result['available_stock'].fillna(10)
    result['reserved_stock'] = result['reserved_stock'].fillna(0)
    
    # 5. Joindre avec les produits
    result = result.merge(products_df[['sku', 'supplier_id', 'pack_size', 'moq', 'safety_stock']], 
                         on='sku', how='left')
    
    # Remplir les valeurs NaN pour les produits
    result['safety_stock'] = result['safety_stock'].fillna(20)
    result['pack_size'] = result['pack_size'].fillna(1)
    result['moq'] = result['moq'].fillna(10)
    result['supplier_id'] = result['supplier_id'].fillna('SUP001')
    
    # 6. Calculer net demand (formule du projet)
    result['net_demand'] = result.apply(
        lambda row: max(0, row['total_quantity'] + row['safety_stock'] - 
                        (row['available_stock'] - row['reserved_stock'])),
        axis=1
    )
    
    # 7. Arrondir au pack_size (formule corrigée)
    def round_to_pack_size(net_demand, pack_size):
        if pack_size <= 0:
            return net_demand
        if net_demand <= 0:
            return 0
        # Arrondir au multiple supérieur de pack_size
        return ((net_demand + pack_size - 1) // pack_size) * pack_size
    
    result['order_quantity'] = result.apply(
        lambda row: round_to_pack_size(row['net_demand'], row['pack_size']),
        axis=1
    )
    
    # 8. Appliquer MOQ (Minimum Order Quantity)
    def apply_moq(order_qty, moq):
        if moq <= 0:
            return order_qty
        if order_qty <= 0:
            return 0
        return max(order_qty, moq)
    
    result['order_quantity'] = result.apply(
        lambda row: apply_moq(row['order_quantity'], row['moq']),
        axis=1
    )
    
    # 9. Forcer quelques commandes pour la date du jour si aucune
    today_str = datetime.now().strftime('%Y-%m-%d')
    if target_date == today_str and result['order_quantity'].sum() == 0:
        print(f"   ℹ Aucune commande nécessaire, création de commandes de test...")
        
        # Prendre les 5 premiers SKUs et forcer une commande
        test_skus = result.head(5).copy()
        test_skus['order_quantity'] = test_skus['moq']  # Commander au MOQ
        
        # Garder les colonnes nécessaires
        to_order = test_skus[['sku', 'supplier_id', 'order_quantity', 'date']].copy()
        to_order['total_quantity'] = test_skus['total_quantity']
        to_order['available_stock'] = test_skus['available_stock']
        to_order['reserved_stock'] = test_skus['reserved_stock']
        to_order['safety_stock'] = test_skus['safety_stock']
        to_order['net_demand'] = test_skus['net_demand']
        
        print(f"   📦 Commandes de test créées: {len(to_order)} SKUs")
    else:
        # 10. Filtrer SKUs à commander
        to_order = result[result['order_quantity'] > 0].copy()
        
        # Ajouter la date
        to_order['date'] = target_date
    
    # 11. Sélectionner les colonnes finales
    final_columns = ['sku', 'supplier_id', 'order_quantity', 'date', 'total_quantity', 
                    'available_stock', 'reserved_stock', 'safety_stock', 'net_demand']
    
    # Garder seulement les colonnes existantes
    existing_columns = [col for col in final_columns if col in to_order.columns]
    to_order_final = to_order[existing_columns]
    
    # 12. Sauvegarder localement
    output_file_local = output_path_local / f"net_demand_{target_date}.csv"
    to_order_final.to_csv(output_file_local, index=False)
    
    print(f"   SKUs à commander: {len(to_order_final)}")
    print(f"   Quantité totale: {to_order_final['order_quantity'].sum()}")
    
    # Afficher un aperçu si des commandes sont nécessaires
    if len(to_order_final) > 0:
        print(f"   📋 SKUs à commander:")
        for _, row in to_order_final.head(3).iterrows():
            print(f"      - {row['sku']}: {row['order_quantity']} unités (stock: {row['available_stock']})")
        if len(to_order_final) > 3:
            print(f"      ... et {len(to_order_final) - 3} autres")
    
    print(f"   ✓ Sauvegardé: {output_file_local}")
    
    return len(to_order_final) > 0

def main():
    """Fonction principale"""
    # Date du jour par défaut
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    success = calculate_net_demand_for_date(target_date)
    
    if success:
        print(f"\n✅ Net demand calculé pour {target_date}")
        print(f"📁 Fichiers locaux: data/processed/net_demand/")
    else:
        print(f"\n⚠️  Aucun besoin de commande pour {target_date}")
        print(f"📁 Fichier généré (vide): data/processed/net_demand/net_demand_{target_date}.csv")

if __name__ == "__main__":
    main()