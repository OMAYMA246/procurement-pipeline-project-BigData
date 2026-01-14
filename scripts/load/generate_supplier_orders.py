"""
Génération des commandes fournisseurs pour une date spécifique
Lit le fichier net_demand et génère les fichiers JSON par fournisseur
"""

import pandas as pd
import json
from pathlib import Path
import sys
from datetime import datetime
import uuid

def generate_supplier_orders_for_date(target_date):
    """Génère les commandes fournisseurs pour une date spécifique"""
    print(f"=== Génération des Commandes Fournisseurs pour {target_date} ===\n")
    
    # Chemins
    net_demand_file = Path(f'data/processed/net_demand/net_demand_{target_date}.csv')
    output_base = Path('data/output/supplier_orders')
    
    # Vérifier si le fichier net_demand existe
    if not net_demand_file.exists():
        print(f"❌ Fichier net_demand non trouvé: {net_demand_file}")
        print(f"ℹ Création d'un répertoire vide pour {target_date}")
        
        # Créer un répertoire vide pour la date
        output_dir = output_base / target_date
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"✓ Répertoire créé: {output_dir}")
        
        return False
    
    print(f"📅 Traitement du {target_date}...")
    
    try:
        # Lire net demand
        demand_df = pd.read_csv(net_demand_file)
        
        if len(demand_df) == 0:
            print(f"   ⚠ Aucune commande pour {target_date}")
            print(f"   ℹ Création d'un répertoire vide")
            
            # Créer un répertoire vide pour la date
            output_dir = output_base / target_date
            output_dir.mkdir(parents=True, exist_ok=True)
            
            return False
        
        # Afficher des informations de débogage
        print(f"   📊 Données chargées: {len(demand_df)} lignes")
        
        # Vérifier les colonnes nécessaires
        required_columns = ['supplier_id', 'order_quantity']
        missing_columns = [col for col in required_columns if col not in demand_df.columns]
        
        if missing_columns:
            print(f"   ❌ Colonnes manquantes: {missing_columns}")
            print(f"   ℹ Colonnes disponibles: {list(demand_df.columns)}")
            return False
        
        # Créer répertoire de sortie
        output_dir = output_base / target_date
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Nettoyer l'ancien contenu du répertoire
        for old_file in output_dir.glob('*.json'):
            old_file.unlink()
        
        # Grouper par fournisseur
        suppliers = demand_df.groupby('supplier_id')
        
        print(f"   👥 Fournisseurs trouvés: {len(suppliers)}")
        
        orders_generated = 0
        total_skus = 0
        total_quantity = 0
        all_suppliers = []
        
        for supplier_id, group in suppliers:
            items = []
            
            # Vérifier si product_name existe
            has_product_name = 'product_name' in group.columns
            
            for _, row in group.iterrows():
                item_data = {
                    'sku': str(row['sku']),
                    'order_quantity': int(row['order_quantity']),
                    'unit': 'pcs'
                }
                
                # Ajouter product_name si disponible
                if has_product_name and pd.notna(row['product_name']):
                    item_data['product_name'] = str(row['product_name'])
                else:
                    # Générer un nom de produit basé sur SKU
                    item_data['product_name'] = f"Produit {row['sku']}"
                
                # Ajouter d'autres informations si disponibles
                if 'total_quantity' in row and pd.notna(row['total_quantity']):
                    item_data['demand_quantity'] = int(row['total_quantity'])
                if 'net_demand' in row and pd.notna(row['net_demand']):
                    item_data['calculated_demand'] = int(row['net_demand'])
                if 'available_stock' in row and pd.notna(row['available_stock']):
                    item_data['current_stock'] = int(row['available_stock'])
                if 'pack_size' in row and pd.notna(row['pack_size']):
                    item_data['pack_size'] = int(row['pack_size'])
                if 'moq' in row and pd.notna(row['moq']):
                    item_data['min_order_quantity'] = int(row['moq'])
                
                items.append(item_data)
            
            total_skus += len(items)
            supplier_total = sum(item['order_quantity'] for item in items)
            total_quantity += supplier_total
            
            # Créer la commande
            order_id = str(uuid.uuid4())[:8].upper()
            order = {
                'order_id': f"ORD-{order_id}",
                'supplier_id': str(supplier_id),
                'order_date': target_date,
                'order_reference': f"ORD-{target_date.replace('-', '')}-{str(supplier_id).replace('SUP', '')}",
                'delivery_date': calculate_delivery_date(target_date),
                'total_items': len(items),
                'total_quantity': supplier_total,
                'status': 'generated',
                'items': items,
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'pipeline_version': '1.0',
                    'target_date': target_date
                }
            }
            
            # Sauvegarder en JSON
            filename = output_dir / f"supplier_{supplier_id}_order_{target_date}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(order, f, indent=2, ensure_ascii=False)
            
            orders_generated += 1
            all_suppliers.append(str(supplier_id))
            
            print(f"   ✓ Fournisseur {supplier_id}: {len(items)} SKUs, {supplier_total} unités")
        
        # Afficher le résumé pour cette date
        print(f"\n📊 RÉSUMÉ POUR {target_date}")
        print(f"   • Fichiers générés: {orders_generated}")
        print(f"   • Fournisseurs: {', '.join(all_suppliers)}")
        print(f"   • SKUs commandés: {total_skus}")
        print(f"   • Unités commandées: {total_quantity}")
        print(f"   • Répertoire: {output_dir}/")
        
        return orders_generated > 0
        
    except Exception as e:
        print(f"❌ Erreur lors du traitement: {e}")
        import traceback
        traceback.print_exc()
        return False

def calculate_delivery_date(order_date_str, lead_time_days=2):
    """Calcule la date de livraison estimée"""
    try:
        order_date = datetime.strptime(order_date_str, '%Y-%m-%d')
        # Utiliser timedelta au lieu de pd.Timedelta
        from datetime import timedelta
        delivery_date = order_date + timedelta(days=lead_time_days)
        return delivery_date.strftime('%Y-%m-%d')
    except:
        # Fallback si erreur de calcul
        return order_date_str

def main():
    """Fonction principale"""
    # Date du jour par défaut
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    success = generate_supplier_orders_for_date(target_date)
    
    if success:
        print(f"\n✅ Commandes fournisseurs générées pour {target_date}")
        print(f"📁 Répertoire de sortie: data/output/supplier_orders/{target_date}/")
        
        # Afficher la liste des fichiers générés
        output_dir = Path(f"data/output/supplier_orders/{target_date}")
        if output_dir.exists():
            json_files = list(output_dir.glob("*.json"))
            if json_files:
                print(f"📄 Fichiers générés:")
                for json_file in json_files:
                    print(f"   • {json_file.name}")
    else:
        print(f"\n⚠️  Aucune commande générée pour {target_date}")
        print(f"📁 Répertoire créé: data/output/supplier_orders/{target_date}/")

if __name__ == "__main__":
    main()