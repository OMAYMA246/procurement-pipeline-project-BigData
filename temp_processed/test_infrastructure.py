#!/usr/bin/env python3
"""
Script de test pour l'infrastructure de procurement pipeline
Teste les connexions à PostgreSQL, HDFS et Trino
"""

import os
import sys
import requests
import psycopg2
from psycopg2 import sql
import subprocess
import json

def test_trino_connection():
    """Test la connexion à Trino"""
    print("\n" + "="*60)
    print("TEST TRINO")
    print("="*60)
    try:
        url = "http://trino:8080/v1/info"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Trino connecté avec succès")
            print(f"   Version: {data['nodeVersion']['version']}")
            print(f"   Coordinator: {data['coordinator']}")
            return True
        else:
            print(f"❌ Erreur Trino: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Erreur de connexion Trino: {e}")
        return False

def test_postgres_connection():
    """Test la connexion à PostgreSQL"""
    print("\n" + "="*60)
    print("TEST POSTGRESQL")
    print("="*60)
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "procurement_db"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres")
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ PostgreSQL connecté avec succès")
        print(f"   Version: {version.split(',')[0]}")
        
        # Créer une table de test
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_suppliers (
                supplier_id SERIAL PRIMARY KEY,
                supplier_name VARCHAR(255) NOT NULL,
                country VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print(f"   Table 'test_suppliers' créée/vérifiée")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Erreur de connexion PostgreSQL: {e}")
        return False

def test_hdfs_connection():
    """Test la connexion à HDFS"""
    print("\n" + "="*60)
    print("TEST HDFS")
    print("="*60)
    try:
        # Tester la connexion au Namenode UI
        namenode_host = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000").replace("hdfs://", "").replace(":9000", "")
        ui_url = f"http://{namenode_host}:9870/"
        
        response = requests.get(ui_url, timeout=5, verify=False)
        print(f"✅ HDFS connecté avec succès")
        print(f"   Namenode: {namenode_host}")
        print(f"   NameNode UI: http://{namenode_host}:9870/")
        print(f"   Status Code: {response.status_code}")
        print(f"   Port HDFS: 9000")
        return True
    except Exception as e:
        print(f"❌ Erreur de connexion HDFS: {e}")
        return False

def main():
    """Fonction principale"""
    print("\n" + "🔍 "*30)
    print("TEST D'INFRASTRUCTURE - PROCUREMENT PIPELINE")
    print("🔍 "*30)
    
    results = {
        'trino': test_trino_connection(),
        'postgres': test_postgres_connection(),
        'hdfs': test_hdfs_connection()
    }
    
    print("\n" + "="*60)
    print("RÉSUMÉ")
    print("="*60)
    
    for service, status in results.items():
        status_str = "✅ OK" if status else "❌ ERREUR"
        print(f"{service.upper():15} {status_str}")
    
    all_passed = all(results.values())
    print("\n" + "="*60)
    if all_passed:
        print("✅ TOUS LES TESTS SONT PASSÉS")
    else:
        print("❌ CERTAINS TESTS ONT ÉCHOUÉ")
    print("="*60 + "\n")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())