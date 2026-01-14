"""
Pipeline hybride utilisant Trino quand disponible, sinon Python
"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path
import os
import argparse
from scripts.utils.trino_client import get_trino_client

class HybridProcurementPipeline:
    
    def __init__(self, target_date=None, use_trino=True):
        self.start_time = datetime.now()
        self.steps_completed = 0
        self.total_steps = 6
        
        # Date cible
        if target_date:
            self.target_date = target_date
        else:
            self.target_date = datetime.now().date()
        
        self.use_trino = use_trino
        self.trino_client = get_trino_client() if use_trino else None
        self.trino_available = False
        
        if use_trino:
            print(f"⚡ Mode: {'Trino SQL' if self.trino_client and self.trino_client.health_check() else 'Python (Trino indisponible)'}")
        
    def print_header(self):
        print(f"""
╔══════════════════════════════════════════════════════════════╗
║          PIPELINE PROCUREMENT - MODE HYBRIDE                 ║
║          Date: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}                         ║
║          Date traitement: {self.target_date}                                ║
║          Moteur: {'Trino SQL distribué' if self.use_trino and self.trino_available else 'Python/Pandas'}                        ║
╚══════════════════════════════════════════════════════════════╝
        """)
    
    def print_step(self, step_num, description):
        print(f"\n{'='*70}")
        print(f"  ÉTAPE {step_num}/{self.total_steps}: {description}")
        print(f"{'='*70}")
    
    def transfer_raw_data_to_hdfs(self):
        """Transfère les données brutes vers HDFS"""
        print(f"\n📤 Transfert des données brutes vers HDFS...")
        
        target_date_str = str(self.target_date)
        
        # Transfert des commandes
        orders_source = Path(f"data/raw/orders/{target_date_str}")
        orders_hdfs = f"/procurement/raw/orders/{target_date_str}"
        
        transferred = 0
        
        if orders_source.exists():
            # Créer le répertoire HDFS
            cmd_mkdir = ['hadoop', 'fs', '-mkdir', '-p', orders_hdfs]
            subprocess.run(cmd_mkdir, capture_output=True)
            
            # Transférer chaque fichier CSV
            for csv_file in orders_source.glob("*.csv"):
                cmd_put = ['hadoop', 'fs', '-put', '-f', str(csv_file), f"{orders_hdfs}/"]
                result = subprocess.run(cmd_put, capture_output=True, text=True)
                if result.returncode == 0:
                    print(f"   ✓ Commandes: {csv_file.name}")
                    transferred += 1
                else:
                    print(f"   ❌ Erreur {csv_file.name}: {result.stderr[:100]}")
        else:
            print(f"   ⚠ Aucune donnée commandes pour {target_date_str}")
        
        # Transfert du stock
        stock_source = Path(f"data/raw/stock/{target_date_str}")
        stock_hdfs = f"/procurement/raw/stock/{target_date_str}"
        
        if stock_source.exists():
            cmd_mkdir = ['hadoop', 'fs', '-mkdir', '-p', stock_hdfs]
            subprocess.run(cmd_mkdir, capture_output=True)
            
            for csv_file in stock_source.glob("*.csv"):
                cmd_put = ['hadoop', 'fs', '-put', '-f', str(csv_file), f"{stock_hdfs}/"]
                result = subprocess.run(cmd_put, capture_output=True, text=True)
                if result.returncode == 0:
                    print(f"   ✓ Stock: {csv_file.name}")
                    transferred += 1
        else:
            print(f"   ⚠ Aucune donnée stock pour {target_date_str}")
        
        print(f"   📊 Total fichiers transférés: {transferred}")
        return transferred > 0
    
    def aggregate_orders(self):
        """Agrège les commandes (avec Trino si disponible)"""
        print(f"\n🔍 Agrégation des commandes...")
        
        target_date_str = str(self.target_date)
        
        if self.use_trino and self.trino_client and self.trino_client.health_check():
            try:
                # 1. Créer les tables externes dans Trino
                self.trino_client.create_external_tables(target_date_str)
                
                # 2. Exécuter l'agrégation avec Trino
                results = self.trino_client.aggregate_orders_with_trino(target_date_str)
                
                if results is not None:
                    print(f"✅ Agrégation Trino réussie: {len(results)} SKUs")
                    
                    # Sauvegarder les résultats localement
                    output_dir = Path("data/processed/aggregated_orders")
                    output_dir.mkdir(parents=True, exist_ok=True)
                    
                    output_file = output_dir / f"aggregated_orders_{target_date_str}.csv"
                    
                    # Ajouter la date aux résultats
                    results['date'] = target_date_str
                    results.to_csv(output_file, index=False)
                    
                    print(f"   📊 Top 5 SKUs:")
                    for i, row in results.head(5).iterrows():
                        print(f"      • {row['sku']}: {row['total_quantity']} unités")
                    
                    self.trino_available = True
                    return True
                    
            except Exception as e:
                print(f"❌ Erreur Trino: {e}")
                print("   ↳ Fallback vers Python...")
        
        # Fallback vers Python
        return self.run_python_script("scripts/transformation/aggregate_orders.py")
    
    def calculate_net_demand(self):
        """Calcule le net demand (avec Trino si disponible)"""
        print(f"\n🧮 Calcul du net demand...")
        
        target_date_str = str(self.target_date)
        
        if self.use_trino and self.trino_available:
            try:
                results = self.trino_client.calculate_net_demand_with_trino(target_date_str)
                
                if results is not None:
                    print(f"✅ Calcul net demand Trino: {len(results)} SKUs à commander")
                    
                    # Sauvegarder les résultats
                    output_dir = Path("data/processed/net_demand")
                    output_dir.mkdir(parents=True, exist_ok=True)
                    
                    output_file = output_dir / f"net_demand_{target_date_str}.csv"
                    results['date'] = target_date_str
                    results.to_csv(output_file, index=False)
                    
                    if len(results) > 0:
                        total_qty = results['order_quantity'].sum()
                        print(f"   📦 Total unités: {total_qty}")
                        
                        # Afficher par fournisseur
                        suppliers = results.groupby('supplier_id')['order_quantity'].sum()
                        for supplier, qty in suppliers.items():
                            print(f"   • Fournisseur {supplier}: {qty} unités")
                    
                    return True
                    
            except Exception as e:
                print(f"❌ Erreur Trino: {e}")
                print("   ↳ Fallback vers Python...")
        
        # Fallback vers Python
        return self.run_python_script("scripts/transformation/calculate_net_demand.py")
    
    def generate_supplier_orders(self):
        """Génère les commandes fournisseurs"""
        print(f"\n📄 Génération des commandes fournisseurs...")
        return self.run_python_script("scripts/transformation/generate_supplier_orders.py")
    
    def run_python_script(self, script_path):
        """Exécute un script Python"""
        try:
            cmd = [sys.executable, script_path, str(self.target_date)]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True
            )
            print(result.stdout)
            if result.stderr and not result.stderr.startswith("Warning"):
                print(f"⚠️  Erreur: {result.stderr[:200]}")
            return result.returncode == 0
        except Exception as e:
            print(f"❌ Erreur exécution script: {e}")
            return False
    
    def transfer_to_hdfs(self, local_path, hdfs_path, description):
        """Transfère les résultats vers HDFS"""
        print(f"\n📤 Transfert vers HDFS: {description}")
        
        local_path = Path(local_path)
        if not local_path.exists():
            print(f"   ⚠ Chemin local inexistant: {local_path}")
            return False
        
        target_date_str = str(self.target_date)
        files_transferred = 0
        
        try:
            # Parcourir récursivement
            for root, dirs, files in os.walk(str(local_path)):
                for file in files:
                    if target_date_str in file:
                        local_file = Path(root) / file
                        relative_path = os.path.relpath(str(local_file), str(local_path))
                        hdfs_file_path = f"{hdfs_path}/{relative_path}"
                        
                        # Créer répertoire HDFS
                        hdfs_dir = os.path.dirname(hdfs_file_path)
                        if hdfs_dir:
                            cmd_mkdir = ['hadoop', 'fs', '-mkdir', '-p', hdfs_dir]
                            subprocess.run(cmd_mkdir, capture_output=True)
                        
                        # Transférer
                        cmd_put = ['hadoop', 'fs', '-put', '-f', str(local_file), hdfs_file_path]
                        result = subprocess.run(cmd_put, capture_output=True, text=True)
                        
                        if result.returncode == 0:
                            print(f"   ✓ {relative_path}")
                            files_transferred += 1
                        else:
                            print(f"   ❌ {relative_path}: {result.stderr[:100]}")
            
            print(f"   📊 Fichiers transférés: {files_transferred}")
            return files_transferred > 0
            
        except Exception as e:
            print(f"❌ Erreur transfert HDFS: {e}")
            return False
    
    def run(self):
        """Exécute le pipeline complet"""
        self.print_header()
        
        # ÉTAPE 1: Transfert données brutes vers HDFS
        self.print_step(1, f"Transfert données brutes - {self.target_date}")
        if self.transfer_raw_data_to_hdfs():
            self.steps_completed += 1
        
        # ÉTAPE 2: Agrégation
        self.print_step(2, f"Agrégation commandes - {self.target_date}")
        if self.aggregate_orders():
            self.steps_completed += 1
        
        # ÉTAPE 3: Calcul net demand
        self.print_step(3, f"Calcul net demand - {self.target_date}")
        if self.calculate_net_demand():
            self.steps_completed += 1
        
        # ÉTAPE 4: Génération commandes
        self.print_step(4, f"Génération commandes - {self.target_date}")
        if self.generate_supplier_orders():
            self.steps_completed += 1
        
        # ÉTAPE 5: Transfert résultats vers HDFS
        self.print_step(5, "Transfert résultats vers HDFS")
        if self.transfer_to_hdfs(
            "data/processed/aggregated_orders",
            "/procurement/processed/aggregated_orders",
            "Commandes agrégées"
        ) and self.transfer_to_hdfs(
            "data/processed/net_demand",
            "/procurement/processed/net_demand",
            "Net demand"
        ) and self.transfer_to_hdfs(
            "data/output/supplier_orders",
            "/procurement/output/supplier_orders",
            "Commandes fournisseurs"
        ):
            self.steps_completed += 1
        
        # ÉTAPE 6: Résumé
        self.print_step(6, "Résumé de l'exécution")
        self.print_summary()
        self.steps_completed += 1
        
        self.print_final_summary()
    
    def print_summary(self):
        """Affiche le résumé"""
        engine = "Trino SQL" if self.trino_available else "Python/Pandas"
        
        print(f"""
📊 RÉSUMÉ DU PIPELINE :
────────────────────────────────────
• Date traitée     : {self.target_date}
• Moteur utilisé   : {engine}
• Étapes réussies  : {self.steps_completed}/{self.total_steps}
────────────────────────────────────
        """)
    
    def print_final_summary(self):
        """Affiche le résumé final"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        engine_status = "TRINO SQL" if self.trino_available else "PYTHON"
        
        print(f"""
╔══════════════════════════════════════════════════════════════╗
║                     RÉSUMÉ FINAL                             ║
╠══════════════════════════════════════════════════════════╣
║ Mode            : {engine_status:45} ║
║ Date traitée    : {str(self.target_date):45} ║
║ Étapes réussies : {f'{self.steps_completed}/{self.total_steps}':45} ║
║ Durée totale    : {f'{duration:.2f}s':45} ║
╚══════════════════════════════════════════════════════════╝

{'🎉  PIPELINE EXÉCUTÉ AVEC SUCCÈS !' if self.steps_completed == self.total_steps else '⚠️   Pipeline complété avec des avertissements'}
        """)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Pipeline procurement hybride')
    parser.add_argument('--date', type=str, default=None,
                       help='Date à traiter (YYYY-MM-DD)')
    parser.add_argument('--no-trino', action='store_true',
                       help='Désactiver Trino (forcer Python)')
    
    args = parser.parse_args()
    
    # Parser la date
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except:
            print("❌ Format date invalide")
            sys.exit(1)
    
    use_trino = not args.no_trino
    
    pipeline = HybridProcurementPipeline(target_date, use_trino=use_trino)
    
    try:
        pipeline.run()
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)