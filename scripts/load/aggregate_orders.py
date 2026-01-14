"""
Script d'agrégation des commandes pour une date spécifique
Calcule la demande totale par SKU pour un jour donné
"""

import pandas as pd
import os
from pathlib import Path
import sys
from datetime import datetime
import yaml

# Chargement configuration
config_path = 'config/config.yaml'
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

def aggregate_orders_for_date(target_date):
    """Agrège les commandes pour une date spécifique"""
    print(f"=== Agrégation des commandes pour {target_date} ===\n")
    
    # Chemins
    orders_path = Path(f"data/raw/orders/{target_date}")
    output_path_local = Path("data/processed/aggregated_orders")
    
    os.makedirs(output_path_local, exist_ok=True)
    
    print(f"📅 Traitement du {target_date}...")
    
    # Vérifier si le dossier de date existe
    if not orders_path.exists():
        print(f"   ⚠ Dossier non trouvé: {orders_path}")
        print(f"   ℹ Création d'un fichier d'agrégation vide")
        
        # Créer un DataFrame vide avec les colonnes attendues
        aggregated = pd.DataFrame(columns=['sku', 'total_quantity', 'product_name', 'date'])
        aggregated['date'] = target_date
        
        # Sauvegarder localement
        output_file_local = output_path_local / f"aggregated_orders_{target_date}.csv"
        aggregated.to_csv(output_file_local, index=False)
        print(f"   ✓ Fichier vide créé: {output_file_local}")
        
        return False
    
    # Lire tous les CSV du jour
    all_orders = []
    csv_files = list(orders_path.glob("*.csv"))
    
    if not csv_files:
        print(f"   ⚠ Aucun fichier CSV trouvé dans {orders_path}")
        print(f"   ℹ Création d'un fichier d'agrégation vide")
        
        aggregated = pd.DataFrame(columns=['sku', 'total_quantity', 'product_name', 'date'])
        aggregated['date'] = target_date
        
        output_file_local = output_path_local / f"aggregated_orders_{target_date}.csv"
        aggregated.to_csv(output_file_local, index=False)
        print(f"   ✓ Fichier vide créé: {output_file_local}")
        
        return False
    
    print(f"   📂 Fichiers trouvés: {len(csv_files)}")
    
    total_lines = 0
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            total_lines += len(df)
            
            # Vérifier les colonnes requises
            required_columns = ['sku', 'quantity']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"   ⚠ Fichier {csv_file.name} manque colonnes: {missing_columns}")
                continue
            
            all_orders.append(df)
            
        except Exception as e:
            print(f"   ⚠ Erreur lecture {csv_file.name}: {e}")
            continue
    
    if not all_orders:
        print(f"   ⚠ Aucune donnée valide trouvée pour {target_date}")
        
        aggregated = pd.DataFrame(columns=['sku', 'total_quantity', 'product_name', 'date'])
        aggregated['date'] = target_date
        
        output_file_local = output_path_local / f"aggregated_orders_{target_date}.csv"
        aggregated.to_csv(output_file_local, index=False)
        print(f"   ✓ Fichier vide créé: {output_file_local}")
        
        return False
    
    # Concaténer toutes les données
    orders_df = pd.concat(all_orders, ignore_index=True)
    print(f"   📊 Total lignes: {total_lines}")
    print(f"   📊 Lignes chargées: {len(orders_df)}")
    
    # Vérifier si la colonne 'product_name' existe
    has_product_name = 'product_name' in orders_df.columns
    
    # Agrégation par SKU
    if has_product_name:
        # Si product_name existe, garder le premier
        aggregated = orders_df.groupby('sku').agg({
            'quantity': 'sum',
            'product_name': 'first'
        }).reset_index()
        
        aggregated.columns = ['sku', 'total_quantity', 'product_name']
    else:
        # Sinon, juste agréger la quantité
        aggregated = orders_df.groupby('sku').agg({
            'quantity': 'sum'
        }).reset_index()
        
        aggregated.columns = ['sku', 'total_quantity']
        # Ajouter une colonne product_name vide
        aggregated['product_name'] = ""
    
    # Ajouter la date
    aggregated['date'] = target_date
    
    print(f"   🔢 SKUs distincts: {len(aggregated)}")
    print(f"   📦 Quantité totale: {aggregated['total_quantity'].sum()}")
    
    # Sauvegarder localement
    output_file_local = output_path_local / f"aggregated_orders_{target_date}.csv"
    aggregated.to_csv(output_file_local, index=False)
    print(f"   💾 Sauvegardé: {output_file_local}")
    
    # Aperçu des données
    if len(aggregated) > 0:
        print(f"   📋 Top 5 SKUs:")
        top_skus = aggregated.nlargest(5, 'total_quantity')[['sku', 'total_quantity']]
        for _, row in top_skus.iterrows():
            print(f"      - {row['sku']}: {row['total_quantity']} unités")
    
    return True

def main():
    """Fonction principale"""
    # Date du jour par défaut
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    success = aggregate_orders_for_date(target_date)
    
    if success:
        print(f"\n✅ Agrégation complète pour {target_date}")
        print(f"📁 Fichiers locaux: data/processed/aggregated_orders/")
    else:
        print(f"\n⚠️  Agrégation terminée avec données vides pour {target_date}")
        print(f"📁 Fichier généré: data/processed/aggregated_orders/aggregated_orders_{target_date}.csv")

if __name__ == "__main__":
    main()