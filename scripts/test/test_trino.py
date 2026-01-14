"""
Script pour tester la connexion et les fonctionnalités Trino
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.trino_client import get_trino_client

def test_trino_connection():
    """Teste la connexion à Trino"""
    print("🧪 Test de connexion Trino...")
    
    client = get_trino_client()
    
    if not client or not client.health_check():
        print("❌ Trino n'est pas accessible")
        print("\n🔧 Vérifiez que:")
        print("   1. Le service Trino est démarré dans Docker")
        print("   2. Les catalogues sont configurés dans config/presto/catalog/")
        print("   3. Trino écoute sur le port 8080")
        return False
    
    print("✅ Trino est accessible")
    
    # Tester les catalogues
    print("\n🔍 Test des catalogues...")
    
    # Catalogue HDFS
    try:
        hdfs_query = "SHOW SCHEMAS IN hdfs"
        hdfs_schemas = client.execute_query(hdfs_query)
        if hdfs_schemas is not None:
            print(f"✅ Catalogue HDFS: {len(hdfs_schemas)} schémas")
        else:
            print("⚠️  Catalogue HDFS non configuré")
    except Exception as e:
        print(f"❌ Erreur catalogue HDFS: {e}")
    
    # Catalogue PostgreSQL
    try:
        pg_query = "SHOW SCHEMAS IN postgresql"
        pg_schemas = client.execute_query(pg_query)
        if pg_schemas is not None:
            print(f"✅ Catalogue PostgreSQL: {len(pg_schemas)} schémas")
        else:
            print("⚠️  Catalogue PostgreSQL non configuré")
    except Exception as e:
        print(f"❌ Erreur catalogue PostgreSQL: {e}")
    
    # Configuration initiale
    print("\n🔧 Configuration des schémas...")
    client.setup_catalogs_and_schemas()
    
    # Test avec des données
    print("\n📊 Test avec données de démonstration...")
    
    # Créer une table de test
    test_query = """
    CREATE TABLE IF NOT EXISTS hdfs.procurement_raw.test_table (
        id INTEGER,
        name VARCHAR,
        value DOUBLE
    )
    WITH (
        format = 'CSV',
        skip_header_line_count = 1,
        external_location = 'hdfs://namenode:9000/procurement/test/'
    )
    """
    
    client.execute_query(test_query, fetch=False)
    print("✅ Table de test créée")
    
    # Vérifier la table
    show_query = "SHOW TABLES IN hdfs.procurement_raw"
    tables = client.execute_query(show_query)
    if tables is not None:
        print(f"📋 Tables dans hdfs.procurement_raw: {list(tables['Table'])}")
    
    print("\n🎉 Tests Trino terminés avec succès!")
    return True

if __name__ == "__main__":
    success = test_trino_connection()
    sys.exit(0 if success else 1)