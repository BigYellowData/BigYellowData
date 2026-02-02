# Exercice 6: Workflow Orchestration with Apache Airflow

L'objectif de cette partie additionnelle est de proposer un système entièrement automatisé avec Airflow.

## Architecture

```
ex06_airflow/
├── dags/
│   └── nyc_taxi_pipeline.py    # DAGs Airflow
├── logs/                        # Logs des tâches
├── plugins/                     # Plugins Airflow (si nécessaire)
├── scripts/
│   └── run_exercise.sh         # Script utilitaire
├── Dockerfile                   # Image Airflow personnalisée
└── README.md
```

## Services Airflow

Le docker-compose inclut les services suivants:

| Service | Description | Port |
|---------|-------------|------|
| airflow-postgres | Base de données métadata Airflow | - |
| airflow-init | Initialisation (création DB, utilisateur admin) | - |
| airflow-webserver | Interface web Airflow | 8082 |
| airflow-scheduler | Ordonnanceur des DAGs | - |

## DAGs disponibles

### 1. `nyc_taxi_full_pipeline`
Pipeline complet orchestrant tous les exercices:
- **Ex01**: Téléchargement des données NYC Taxi vers MinIO
- **Ex02**: Nettoyage et transformation avec Spark
- **Ex03**: Création des tables PostgreSQL et chargement
- **Ex05**: Entraînement du modèle ML

**Déclenchement**: Manuel (via UI Airflow)

### 2. `nyc_taxi_monthly_refresh`
Refresh mensuel automatique des données:
- Planifié le 1er de chaque mois à 2h00
- Télécharge les nouvelles données
- Ré-exécute le pipeline complet
- Réentraîne le modèle ML

## Prérequis

Avant d'exécuter les DAGs, les JARs Spark doivent être compilés sur la machine hôte:

```bash
# Compiler tous les JARs
./run_spark_docker.sh ex01_data_retrieval
./run_spark_docker.sh ex02_data_ingestion
./run_spark_docker.sh ex03_sql_table_creation IngestToDWH
```

Ou utiliser le script complet:
```bash
./setup_and_run.sh all
```

## Démarrage

### Option 1: Via setup_and_run.sh
```bash
./setup_and_run.sh ex06
```

### Option 2: Manuellement
```bash
# Démarrer la base Airflow
docker compose up -d airflow-postgres
sleep 5

# Initialiser Airflow
docker compose run --rm airflow-init

# Démarrer les services
docker compose up -d airflow-webserver airflow-scheduler
```

## Accès à l'interface

- **URL**: http://localhost:8082
- **Utilisateur**: admin
- **Mot de passe**: admin

## Utilisation

1. Accéder à l'interface Airflow
2. Activer le DAG `nyc_taxi_full_pipeline`
3. Cliquer sur "Trigger DAG" pour lancer le pipeline
4. Suivre l'exécution via le Graph View

## Flux de données

```
┌─────────────────────────────────────────────────────────────────┐
│                        Airflow DAG                               │
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │  Ex01    │───▶│  Ex02    │───▶│  Ex03    │───▶│  Ex05    │  │
│  │ Download │    │  Clean   │    │  Load    │    │  Train   │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│       │               │               │               │         │
│       ▼               ▼               ▼               ▼         │
│   ┌───────┐      ┌───────┐      ┌───────┐      ┌───────┐       │
│   │ MinIO │      │ MinIO │      │Postgres│     │ Model │       │
│   │ (raw) │      │(clean)│      │  DWH   │     │(.joblib)     │
│   └───────┘      └───────┘      └───────┘      └───────┘       │
└─────────────────────────────────────────────────────────────────┘
```

## Dépannage

### Les DAGs ne s'affichent pas
```bash
# Vérifier les logs du scheduler
docker logs airflow-scheduler

# Vérifier que les DAGs sont bien montés
docker exec airflow-webserver ls /opt/airflow/dags/
```

### Erreur "JAR not found"
Les JARs doivent être précompilés et copiés dans spark-master:
```bash
./run_spark_docker.sh ex01_data_retrieval
```

### Connexion aux conteneurs
```bash
# Accéder au webserver
docker exec -it airflow-webserver bash

# Vérifier les connexions
docker exec airflow-webserver airflow connections list
```
