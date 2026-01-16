# BigYellowData - Quick Start Guide

## 🚀 Démarrage Rapide

### Prérequis
- Docker & Docker Compose
- Java 11+ (pour sbt/Scala local)
- 8GB RAM minimum

### Commande unique pour tout lancer

```bash
# Lancer TOUS les exercices (Ex01 → Ex02 → Ex03)
./setup_and_run.sh all

# OU lancer un exercice spécifique
./setup_and_run.sh ex01   # Data Retrieval vers MinIO
./setup_and_run.sh ex02   # Data Ingestion & Cleaning
./setup_and_run.sh ex03   # SQL Data Warehouse Setup
```

Le script `setup_and_run.sh` fait TOUT automatiquement :
- ✅ Démarre Docker (Spark, MinIO, PostgreSQL, pgAdmin)
- ✅ Crée le bucket MinIO
- ✅ Upload les fichiers nécessaires (taxi_zone_lookup.csv)
- ✅ Lance les exercices Spark
- ✅ Initialise le Data Warehouse PostgreSQL

### 📊 Accès aux Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Spark Master UI** | http://localhost:8081 | - |
| **MinIO Console** | http://localhost:9001 | Voir `.env` (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD) |
| **pgAdmin** | http://localhost:5050 | Email: `admin@admin.com` / Pass: `admin` |
| **PostgreSQL** | localhost:5432 | User: `user_dw` / Pass: `password_dw` / DB: `nyc_data_warehouse` |

### 🗂️ Structure du Projet

```
BigYellowData/
├── setup_and_run.sh          # 🎯 SCRIPT PRINCIPAL (utilisez celui-ci!)
├── run_spark_docker.sh       # Script bas-niveau (utilisé par setup_and_run.sh)
├── docker-compose.yml        # Configuration Docker
├── data/raw/                 # Données brutes (parquet files)
├── ex01_data_retrieval/      # Exercice 1: Upload vers MinIO
├── ex02_data_ingestion/      # Exercice 2: Nettoyage des données
├── ex03_sql_table_creation/  # Exercice 3: Data Warehouse SQL
│   ├── creation.sql          # Création du schéma (constellation)
│   ├── insertion.sql         # Données de référence
│   ├── aggregation.sql       # Tables agrégées
│   └── README.md             # Documentation détaillée du DWH
├── ex04_dashboard/           # Exercice 4: Streamlit Dashboard
└── ex05_ml_prediction_service/ # Exercice 5: Machine Learning
```

### 🔄 Workflow Complet

```
1. Data Lake (MinIO)
   ↓
2. Data Cleaning (Spark Ex02)
   ↓
3. Data Warehouse (PostgreSQL Ex03)
   ↓
4. Dashboard (Streamlit Ex04)
   ↓
5. ML Model (Python Ex05)
```

### 🛠️ Commandes Utiles

```bash
# Voir les logs d'un conteneur
docker logs -f spark-master
docker logs -f postgres-dw

# Vérifier les données dans MinIO
docker exec minio mc ls -r myminio/nyctaxiproject/

# Se connecter à PostgreSQL
docker exec -it postgres-dw psql -U user_dw -d nyc_data_warehouse

# Requête SQL rapide
docker exec -it postgres-dw psql -U user_dw -d nyc_data_warehouse -c "SELECT COUNT(*) FROM dw.fact_trip;"

# Stopper tout
docker-compose down

# Nettoyer les volumes (⚠️ Supprime toutes les données!)
docker-compose down -v
```

### 📈 Vérification Post-Exécution

Après avoir lancé `./setup_and_run.sh all`, vérifiez:

**MinIO**:
```bash
docker exec minio mc ls myminio/nyctaxiproject/
# Devrait afficher:
# - nyc_raw/ (depuis Ex01)
# - dwh/yellow_taxi_refined/ (depuis Ex02)
# - taxi_zone_lookup.csv
```

**PostgreSQL**:
```bash
docker exec postgres-dw psql -U user_dw -d nyc_data_warehouse -c "
SET search_path TO dw;
SELECT
  'dim_vendor' as table_name, COUNT(*) as rows FROM dim_vendor
UNION ALL
SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_trip', COUNT(*) FROM fact_trip;
"
```

Résultat attendu:
- dim_vendor: 4 rows
- dim_location: 265 rows
- dim_date: 3,650 rows
- fact_trip: ~5-10M rows (dépend de Ex02)

### 🐛 Troubleshooting

**Problème**: "Port already in use"
```bash
# Vérifier les ports occupés
docker ps
# Stopper les conteneurs
docker-compose down
```

**Problème**: "Out of memory"
```bash
# Augmenter la mémoire Docker (Docker Desktop Settings)
# Minimum 8GB recommandé
```

**Problème**: "Permission denied"
```bash
chmod +x setup_and_run.sh
chmod +x run_spark_docker.sh
```

**Problème**: Compilation SBT échoue
```bash
cd ex02_data_ingestion
sbt clean
sbt compile
```

### 📝 Notes Importantes

1. **Première exécution**: Plus lente (téléchargement des images Docker)
2. **Données**: Les 3 mois de données = ~200MB
3. **Durée**:
   - Ex01: ~2-3 min
   - Ex02: ~5-10 min (dépend des données)
   - Ex03: ~30 secondes
4. **Persistence**: Les données restent même après `docker-compose down` (sauf si `-v`)

### 🎓 Pour aller plus loin

- **Ex03 Architecture**: Voir [ex03_sql_table_creation/README.md](ex03_sql_table_creation/README.md)
- **Modèle Constellation**: Justification et diagrammes dans le README Ex03
- **Dashboard (Ex04)**: À implémenter avec Streamlit
- **ML (Ex05)**: Prédiction de `total_amount` depuis les parquet MinIO

### ✅ Checklist Avant de Push

- [ ] `./setup_and_run.sh all` fonctionne sans erreur
- [ ] Tous les conteneurs sont UP: `docker ps`
- [ ] MinIO contient les données: `docker exec minio mc ls -r myminio/nyctaxiproject/`
- [ ] PostgreSQL a les tables: Voir "Vérification Post-Exécution"
- [ ] Les logs Spark ne montrent pas d'erreurs critiques

---

**Dernière mise à jour**: 2026-01-14
**Auteur**: Nadir (équipe BigYellowData)
