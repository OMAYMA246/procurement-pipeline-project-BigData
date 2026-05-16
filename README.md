# 🏪 Pipeline de Procurement - Système de Réapprovisionnement Automatisé

[![Big Data](https://img.shields.io/badge/Big%20Data-Hadoop%20%7C%20Trino-blue)](https://github.com)
[![Python](https://img.shields.io/badge/Python-3.11-green)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://www.docker.com/)

## 📋 Description

Projet académique implémentant un **pipeline de données distribué** pour un système de réapprovisionnement automatique dans le secteur de la grande distribution. Ce système collecte les commandes clients, analyse les stocks et génère automatiquement des commandes fournisseurs optimisées.

### 🎯 Objectifs
- Traiter des flux de données distribués avec Hadoop/HDFS
- Implémenter un pipeline batch de bout en bout
- Calculer automatiquement les besoins nets en approvisionnement
- Générer des commandes fournisseurs optimisées

---

## 🏗️ Architecture

### Stack Technique
- **Stockage Distribué** : HDFS (Hadoop 3.3.1)
- **Base de Données** : PostgreSQL 15
- **Moteur de Requêtes** : Trino (anciennement Presto)
- **Metastore** : Hive Metastore 3.1.3
- **Orchestration** : Python 3.11
- **Conteneurisation** : Docker & Docker Compose

### Schéma d'Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                    PIPELINE ARCHITECTURE                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  RAW DATA (HDFS)          PROCESSING           OUTPUT       │
│  ┌──────────────┐        ┌──────────┐        ┌──────────┐  │
│  │   Orders     │───────▶│          │───────▶│ Supplier │  │
│  │   (CSV)      │        │  Python  │        │  Orders  │  │
│  └──────────────┘        │  Scripts │        │  (JSON)  │  │
│                          │          │        └──────────┘  │
│  ┌──────────────┐        │  Trino   │                      │
│  │   Stock      │───────▶│  Queries │                      │
│  │  Snapshots   │        │          │                      │
│  └──────────────┘        └──────────┘                      │
│                                                              │
│  ┌──────────────┐                                           │
│  │ PostgreSQL   │                                           │
│  │ Master Data  │                                           │
│  └──────────────┘                                           │
└─────────────────────────────────────────────────────────────┘
```


## 🚀 Installation et Démarrage

### Prérequis
- Docker Desktop installé
- Docker Compose
- PowerShell (Windows) ou Bash (Linux/Mac)
- 8 GB RAM minimum

### 1. Cloner le projet
```bash
git clone https://github.com/OMAYMA246/procurement-pipeline-project-BigData.git
```

### 2. Démarrer l'infrastructure
```bash
docker-compose up -d
```

Attendre 2-3 minutes que tous les services démarrent.

### 3. Vérifier les services
```bash
docker ps
```
### 8. Exécuter le pipeline complet



```bash
docker exec -it python_env bash 
#definir variable 
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
#run pipline
python scripts/run_pipeline/run_pipeline.py
```


Vous devriez voir 5 conteneurs :
- `procurement_namenode`
- `procurement_datanode1`
- `procurement_datanode2`
- `procurement_postgres`
- `procurement_metastore`
- `procurement_trino`
- `procurement_python`

### 4. Initialiser la base de données
```bash
docker exec -it procurement_python python scripts/data_generation/generate_master_data.py
docker exec -it procurement_python python scripts/data_generation/generate_operational_data.py

```


### 6. Charger les données dans HDFS
```bash
docker exec -it procurement_namenode hdfs dfs -put /data/raw/orders /procurement/raw/
docker exec -it procurement_namenode hdfs dfs -put /data/raw/stock /procurement/raw/
```

### 7. Créer les tables Trino
```bash
docker exec -i procurement_trino trino  scripts/sql/create_orders_view_all_days.sql
docker exec -i procurement_trino trino  scripts/sql/create_stock_view_all_days.sql
```

### 8. Exécuter le pipeline complet



```bash
docker exec -it python_env bash 
#definir variable 
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
#run pipline
python scripts/run_pipeline/run_pipeline.py
```



## 🧮 Logique Métier

### Formule de Calcul du Net Demand
```python
net_demand = max(0, 
    aggregated_orders           # Demande totale
    + safety_stock              # Stock de sécurité
    - (available_stock          # Stock disponible
       - reserved_stock)        # Stock réservé
)
```

### Règles d'Approvisionnement
1. **Pack Size** : Arrondir au multiple supérieur du conditionnement
2. **MOQ (Minimum Order Quantity)** : Respecter la quantité minimale de commande
3. **Safety Stock** : Maintenir un stock de sécurité minimum

---

## 🔍 Commandes Utiles

### Vérifier HDFS
```bash
# Lister les fichiers
docker exec procurement_namenode hdfs dfs -ls /procurement/

# Voir le contenu d'un fichier
docker exec procurement_namenode hdfs dfs -cat /procurement/processed/net_demand/net_demand_2026-01-12.csv

# Taille des répertoires
docker exec procurement_namenode hdfs dfs -du -h /procurement
```

### Requêtes Trino
```bash
# Lancer Trino CLI
docker exec -it procurement_trino trino

# Compter les commandes
docker exec -it procurement_trino trino --execute "SELECT COUNT(*) FROM hive.procurement.orders;"

# Top 10 produits
docker exec -it procurement_trino trino --execute "SELECT sku, SUM(CAST(quantity AS INTEGER)) as total FROM hive.procurement.orders GROUP BY sku ORDER BY total DESC LIMIT 10;"
```

### PostgreSQL
```bash
# Connexion
docker exec -it procurement_postgres psql -U postgres -d procurement_db

# Lister les produits
SELECT * FROM products LIMIT 10;

# Compter les fournisseurs
SELECT COUNT(*) FROM suppliers;
```

---

## 🐛 Dépannage

### Les conteneurs ne démarrent pas
```bash
docker-compose down
docker-compose up -d
docker-compose logs
```

### HDFS en mode safe mode
```bash
docker exec -it procurement_namenode hdfs dfsadmin -safemode leave
```

### Erreur Trino "Schema does not exist"
```bash
# Recréer le schéma
docker exec -it procurement_trino trino --execute "CREATE SCHEMA IF NOT EXISTS hive.procurement;"
```

---

## 📚 Documentation Technique

### Technologies Utilisées

| Technologie | Version | Rôle |
|------------|---------|------|
| HDFS | 3.3.1 | Stockage distribué |
| PostgreSQL | 15 | Base de données relationnelle |
| Trino | 403 | Moteur de requêtes distribuées |
| Hive Metastore | 3.1.3 | Catalogue de métadonnées |
| Python | 3.11 | Orchestration et transformations |
| Pandas | 2.0+ | Manipulation de données |
| Docker | 24.0+ | Conteneurisation |

### Flux de Données
1. **Génération** : Création de données de test avec Faker
2. **Ingestion** : Chargement dans HDFS
3. **Agrégation** : Calcul des totaux par SKU
4. **Calcul** : Net demand avec règles métier
5. **Génération** : Création des commandes fournisseurs
6. **Stockage** : Sauvegarde dans HDFS

---

## 👥 Auteurs

- **Votre Nom** - Projet académique - Université Abdelmalek Essaâdi
- **Module** : Fondements Big Data
- **Encadré par** :Mr. Mohamed El Marouani

---

## 📄 Licence

Ce projet est à usage éducatif uniquement.

---

## Remerciements

- Université Abdelmalek Essaâdi
- École Nationale des Sciences Appliquées - Al-Hoceima
- Département Mathématiques et Informatique

---



---

