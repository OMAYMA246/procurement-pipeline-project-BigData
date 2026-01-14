"""
Client Trino pour le pipeline procurement
Gère les connexions et exécutions de requêtes SQL distribuées
"""

import os
import sys
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuth
import logging

logger = logging.getLogger(__name__)

class TrinoClient:
    """Client pour communiquer avec Trino"""
    
    def __init__(self, host='trino', port=8080, user='trino', catalog='hive', schema='default'):
        """
        Initialise le client Trino
        
        Args:
            host: Hostname du serveur Trino
            port: Port de Trino
            user: Utilisateur Trino
            catalog: Catalogue Trino (par défaut 'hive')
            schema: Schéma par défaut
        """
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.connection = None
        self.connected = False
        
        self._connect()
    
    def _connect(self):
        """Établit la connexion à Trino"""
        try:
            self.connection = connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema,
            )
            self.connected = True
            logger.info(f"✅ Connecté à Trino {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"❌ Erreur connexion Trino: {e}")
            self.connected = False
            return False
    
    def health_check(self):
        """Vérifie que Trino est accessible"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except:
            return False
    
    def execute_query(self, query, fetch_results=True):
        """
        Exécute une requête Trino
        
        Args:
            query: Requête SQL
            fetch_results: Si True, récupère les résultats
        
        Returns:
            DataFrame pandas ou None en cas d'erreur
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            if fetch_results:
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                
                if rows:
                    return pd.DataFrame(rows, columns=columns)
                else:
                    return pd.DataFrame()
            else:
                self.connection.commit()
                return None
                
        except Exception as e:
            logger.error(f"❌ Erreur requête Trino: {e}")
            return None
    
    def create_tables(self):
        """Crée les tables nécessaires au pipeline"""
        try:
            # Table des commandes
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS hive.default.orders (
                    order_id VARCHAR,
                    sku VARCHAR,
                    quantity INTEGER,
                    order_date DATE,
                    supplier_id VARCHAR
                )
            """, fetch_results=False)
            
            # Table du stock
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS hive.default.stock (
                    sku VARCHAR,
                    warehouse_location VARCHAR,
                    available_quantity INTEGER,
                    reserved_quantity INTEGER,
                    stock_date DATE
                )
            """, fetch_results=False)
            
            # Table des SKU
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS hive.default.sku_master (
                    sku VARCHAR,
                    product_name VARCHAR,
                    category VARCHAR,
                    reorder_point INTEGER,
                    lead_time_days INTEGER,
                    supplier_id VARCHAR
                )
            """, fetch_results=False)
            
            logger.info("✅ Tables créées avec succès")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur création tables: {e}")
            return False
    
    def aggregate_orders(self, target_date):
        """
        Agrège les commandes par SKU pour une date donnée
        
        Args:
            target_date: Date au format 'YYYY-MM-DD'
        
        Returns:
            DataFrame avec colonnes: sku, total_quantity
        """
        query = f"""
        SELECT 
            sku,
            COUNT(*) as order_count,
            SUM(quantity) as total_quantity,
            AVG(quantity) as avg_quantity,
            MAX(quantity) as max_quantity
        FROM hive.default.orders
        WHERE DATE(order_date) = DATE('{target_date}')
        GROUP BY sku
        ORDER BY total_quantity DESC
        """
        
        try:
            result = self.execute_query(query)
            logger.info(f"✅ Agrégation orders: {len(result)} SKUs")
            return result
        except Exception as e:
            logger.error(f"❌ Erreur agrégation: {e}")
            return None
    
    def calculate_net_demand(self, target_date):
        """
        Calcule le net demand (demande nette) pour chaque SKU
        
        Net Demand = Total Demand - Available Stock
        
        Args:
            target_date: Date au format 'YYYY-MM-DD'
        
        Returns:
            DataFrame avec colonnes: sku, total_demand, available_stock, 
                                    net_demand, order_quantity, supplier_id
        """
        query = f"""
        WITH daily_demand AS (
            -- Agrégation des demandes par SKU
            SELECT 
                o.sku,
                CAST(SUM(o.quantity) AS INTEGER) as total_demand,
                sm.supplier_id,
                sm.reorder_point,
                sm.lead_time_days
            FROM hive.default.orders o
            LEFT JOIN hive.default.sku_master sm ON o.sku = sm.sku
            WHERE DATE(o.order_date) <= DATE('{target_date}')
            GROUP BY o.sku, sm.supplier_id, sm.reorder_point, sm.lead_time_days
        ),
        current_stock AS (
            -- Stock actuel et réservations
            SELECT 
                sku,
                CAST(COALESCE(SUM(available_quantity), 0) AS INTEGER) as available_stock,
                CAST(COALESCE(SUM(reserved_quantity), 0) AS INTEGER) as reserved_stock
            FROM hive.default.stock
            WHERE DATE(stock_date) = DATE('{target_date}')
            GROUP BY sku
        )
        SELECT 
            dd.sku,
            dd.total_demand,
            COALESCE(cs.available_stock, 0) as available_stock,
            COALESCE(cs.reserved_stock, 0) as reserved_stock,
            -- Net demand = max(Demand - Stock actuel - Stock réservé, 0)
            GREATEST(
                dd.total_demand - COALESCE(cs.available_stock, 0) - COALESCE(cs.reserved_stock, 0),
                0
            ) as net_demand,
            -- Quantité à commander = max(Net demand + safety stock, reorder point)
            GREATEST(
                GREATEST(
                    dd.total_demand - COALESCE(cs.available_stock, 0) - COALESCE(cs.reserved_stock, 0),
                    0
                ) + CAST(dd.reorder_point * 0.1 AS INTEGER),
                COALESCE(dd.reorder_point, 0)
            ) as order_quantity,
            dd.supplier_id,
            dd.lead_time_days
        FROM daily_demand dd
        LEFT JOIN current_stock cs ON dd.sku = cs.sku
        WHERE GREATEST(
            dd.total_demand - COALESCE(cs.available_stock, 0) - COALESCE(cs.reserved_stock, 0),
            0
        ) > 0
        ORDER BY dd.sku
        """
        
        try:
            result = self.execute_query(query)
            if result is not None and len(result) > 0:
                logger.info(f"✅ Net demand calculé: {len(result)} SKUs à commander")
                return result
            else:
                logger.warning(f"⚠️  Aucun net demand pour {target_date}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ Erreur calcul net demand: {e}")
            return None
    
    def get_supplier_orders(self, target_date):
        """
        Récupère les données pour générer les commandes par fournisseur
        
        Args:
            target_date: Date au format 'YYYY-MM-DD'
        
        Returns:
            DataFrame groupé par fournisseur et SKU
        """
        query = f"""
        WITH net_demand_data AS (
            SELECT 
                o.sku,
                sm.supplier_id,
                CAST(SUM(o.quantity) AS INTEGER) as total_demand,
                COALESCE(s.available_stock, 0) as available_stock,
                GREATEST(
                    CAST(SUM(o.quantity) AS INTEGER) - COALESCE(s.available_stock, 0),
                    0
                ) as order_quantity
            FROM hive.default.orders o
            LEFT JOIN hive.default.sku_master sm ON o.sku = sm.sku
            LEFT JOIN (
                SELECT sku, CAST(SUM(available_quantity) AS INTEGER) as available_stock
                FROM hive.default.stock
                WHERE DATE(stock_date) = DATE('{target_date}')
                GROUP BY sku
            ) s ON o.sku = s.sku
            WHERE DATE(o.order_date) <= DATE('{target_date}')
            GROUP BY o.sku, sm.supplier_id, s.available_stock
        )
        SELECT 
            supplier_id,
            sku,
            total_demand,
            available_stock,
            order_quantity
        FROM net_demand_data
        WHERE order_quantity > 0
        ORDER BY supplier_id, sku
        """
        
        try:
            result = self.execute_query(query)
            if result is not None and len(result) > 0:
                logger.info(f"✅ Commandes fournisseurs: {len(result)} lignes")
                return result
            else:
                logger.warning(f"⚠️  Aucune donnée commande pour {target_date}")
                return None
        except Exception as e:
            logger.error(f"❌ Erreur récupération orders: {e}")
            return None
    
    def load_csv_to_trino(self, csv_path, table_name):
        """
        Charge un CSV dans Trino
        
        Args:
            csv_path: Chemin du fichier CSV
            table_name: Nom de la table Trino
        
        Returns:
            True si succès, False sinon
        """
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"📥 Chargement de {len(df)} lignes dans {table_name}")
            
            # Insérer les données
            columns = ','.join(df.columns)
            values_list = []
            
            for _, row in df.iterrows():
                values = ','.join([f"'{v}'" if isinstance(v, str) else str(v) for v in row.values])
                values_list.append(f"({values})")
            
            if values_list:
                values_str = ','.join(values_list)
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES {values_str}"
                self.execute_query(insert_query, fetch_results=False)
                logger.info(f"✅ {len(df)} lignes insérées dans {table_name}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Erreur chargement CSV: {e}")
            return False
    
    def close(self):
        """Ferme la connexion Trino"""
        try:
            if self.connection:
                self.connection.close()
                self.connected = False
                logger.info("✅ Connexion Trino fermée")
        except Exception as e:
            logger.error(f"❌ Erreur fermeture connexion: {e}")


def get_trino_client():
    """Factory function pour créer un client Trino"""
    try:
        client = TrinoClient(
            host=os.getenv('TRINO_HOST', 'trino'),
            port=int(os.getenv('TRINO_PORT', 8080)),
            user=os.getenv('TRINO_USER', 'trino')
        )
        
        if client.health_check():
            return client
        else:
            logger.warning("⚠️  Trino non accessible, retour None")
            return None
    except Exception as e:
        logger.error(f"❌ Impossible créer client Trino: {e}")
        return None