"""
Génération du rapport d'exceptions et anomalies
"""

import pandas as pd
import json
import yaml
import psycopg2
from datetime import datetime
from pathlib import Path

def generate_exception_report():
    print("=== Génération du Rapport d'Exceptions ===\n")
    
    exceptions = []
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    # 1. Vérifier fichiers manquants
    print("1. Vérification des fichiers manquants...")
    orders_path = Path('data/raw/orders')
    expected_stores = 5
    
    for date_folder in sorted(orders_path.iterdir()):
        if date_folder.is_dir():
            csv_files = list(date_folder.glob('*.csv'))
            if len(csv_files) < expected_stores:
                exceptions.append({
                    'date': date_folder.name,
                    'type': 'MISSING_FILES',
                    'severity': 'WARNING',
                    'message': f'Only {len(csv_files)}/{expected_stores} store files found'
                })
                print(f"   ⚠️  {date_folder.name}: {len(csv_files)}/{expected_stores} fichiers")
    
    # 2. Détecter demandes anormales
    print("\n2. Détection des demandes anormales...")
    net_demand_path = Path('data/processed/net_demand')
    
    all_quantities = []
    for demand_file in sorted(net_demand_path.glob('*.csv')):
        df = pd.read_csv(demand_file)
        if len(df) > 0:
            all_quantities.extend(df['order_quantity'].tolist())
    
    if len(all_quantities) > 0:
        mean_qty = pd.Series(all_quantities).mean()
        std_qty = pd.Series(all_quantities).std()
        threshold = mean_qty + 3 * std_qty
        
        for demand_file in sorted(net_demand_path.glob('*.csv')):
            df = pd.read_csv(demand_file)
            if len(df) > 0:
                anomalies = df[df['order_quantity'] > threshold]
                for _, row in anomalies.iterrows():
                    exceptions.append({
                        'date': demand_file.stem.split('_')[-1],
                        'type': 'ABNORMAL_DEMAND',
                        'severity': 'WARNING',
                        'sku': row['sku'],
                        'quantity': int(row['order_quantity']),
                        'message': f"Abnormal order quantity: {int(row['order_quantity'])} units (threshold: {int(threshold)})"
                    })
                    print(f"   ⚠️  {row['sku']}: {int(row['order_quantity'])} unités (seuil: {int(threshold)})")
    
    # 3. Vérifier mapping fournisseurs
    print("\n3. Vérification des mappings fournisseurs...")
    try:
        conn = psycopg2.connect(
            host='procurement_postgres',
            database='procurement_db',
            user='postgres',
            password='postgres'
        )
        
        products_df = pd.read_sql('SELECT sku, supplier_id FROM products WHERE supplier_id IS NULL', conn)
        conn.close()
        
        if len(products_df) > 0:
            exceptions.append({
                'date': date_str,
                'type': 'MISSING_SUPPLIER_MAPPING',
                'severity': 'ERROR',
                'count': len(products_df),
                'message': f'{len(products_df)} products without supplier mapping'
            })
            print(f"   ❌ {len(products_df)} produits sans fournisseur")
        else:
            print(f"   ✓ Tous les produits ont un fournisseur")
    except Exception as e:
        print(f"   ⚠️  Erreur connexion PostgreSQL: {e}")
    
    # 4. Vérifier cohérence stock vs commandes
    print("\n4. Vérification cohérence stock/commandes...")
    stock_path = Path('data/raw/stock')
    
    for date_folder in sorted(orders_path.iterdir()):
        if date_folder.is_dir():
            date_name = date_folder.name
            corresponding_stock = stock_path / date_name
            
            if not corresponding_stock.exists():
                exceptions.append({
                    'date': date_name,
                    'type': 'MISSING_STOCK_SNAPSHOT',
                    'severity': 'ERROR',
                    'message': f'Stock snapshot missing for {date_name}'
                })
                print(f"   ❌ Snapshot stock manquant: {date_name}")
    
    # Générer le rapport
    print("\n5. Génération du rapport final...")
    output_dir = Path('data/logs/exceptions')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    report = {
        'generated_at': datetime.now().isoformat(),
        'pipeline_date': date_str,
        'total_exceptions': len(exceptions),
        'exceptions_by_severity': {
            'ERROR': len([e for e in exceptions if e['severity'] == 'ERROR']),
            'WARNING': len([e for e in exceptions if e['severity'] == 'WARNING'])
        },
        'exceptions_by_type': {},
        'exceptions': exceptions
    }
    
    # Compter par type
    for exc in exceptions:
        exc_type = exc['type']
        report['exceptions_by_type'][exc_type] = report['exceptions_by_type'].get(exc_type, 0) + 1
    
    output_file = output_dir / f'exception_report_{date_str}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"✓ Rapport généré: {output_file}")
    print(f"{'='*60}")
    print(f"\n📊 RÉSUMÉ DES EXCEPTIONS")
    print(f"{'='*60}")
    print(f"Total exceptions détectées: {len(exceptions)}")
    print(f"  • Erreurs (ERROR): {report['exceptions_by_severity']['ERROR']}")
    print(f"  • Avertissements (WARNING): {report['exceptions_by_severity']['WARNING']}")
    
    if len(report['exceptions_by_type']) > 0:
        print(f"\n📋 Par type:")
        for exc_type, count in report['exceptions_by_type'].items():
            print(f"  • {exc_type}: {count}")
    
    if len(exceptions) > 0:
        print(f"\n⚠️  Dernières exceptions détectées:")
        for exc in exceptions[-5:]:
            severity_icon = "❌" if exc['severity'] == 'ERROR' else "⚠️"
            print(f"  {severity_icon} [{exc['severity']}] {exc.get('type', 'UNKNOWN')}")
            print(f"     {exc['message']}")
    else:
        print(f"\n✅ Aucune exception détectée - Pipeline sain!")
    
    print(f"\n{'='*60}")
    
    return report

if __name__ == "__main__":
    try:
        report = generate_exception_report()
    except Exception as e:
        print(f"\n❌ ERREUR FATALE: {e}")
        import traceback
        traceback.print_exc()