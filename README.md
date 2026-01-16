# BigYellowData - NYC Yellow Taxi Data Warehouse

Projet Big Data pour l'analyse des courses de taxis jaunes de New York City.

## Architecture du Projet

```
BigYellowData/
├── ex01_data_retrieval/     # Exercice 1 : Récupération des données
├── ex02_data_ingestion/     # Exercice 2 : Ingestion dans MinIO (Data Lake)
├── ex03_sql_table_creation/ # Exercice 3 : Création du Data Warehouse PostgreSQL
├── ex04_dashboard/          # Exercice 4 : Dashboard Streamlit
├── docker/                  # Configuration Spark
├── docker-compose.yml       # Orchestration des services
├── setup_and_run.sh         # Script principal d'exécution
└── run_spark_docker.sh      # Script d'exécution Spark
```

## Prérequis

- **Docker** et **Docker Compose** installés
- **Git** pour cloner le projet
- Minimum **8 Go de RAM** disponibles
- Ports libres : 5432, 5050, 7077, 8081, 8501, 9000, 9001

## Installation

### 1. Cloner le projet

```bash
git clone <url-du-repo>
cd BigYellowData
```

### 2. Créer le fichier .env

Le fichier `.env` contient les credentials MinIO. Créez-le à la racine du projet :

```bash
cat > .env << 'EOF'
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
MINIO_ENDPOINT=http://minio:9000
EOF
```

> **Important** : Vous pouvez personnaliser les credentials, mais gardez les mêmes valeurs dans tout le projet.

### 3. Rendre les scripts exécutables

```bash
chmod +x setup_and_run.sh run_spark_docker.sh
```

## Lancement des Exercices

### Option 1 : Tout exécuter d'un coup (recommandé pour la première fois)

```bash
./setup_and_run.sh all
```

Cela va :
1. Démarrer l'infrastructure Docker (Spark, MinIO, PostgreSQL, pgAdmin)
2. Exécuter l'Exercice 1 (téléchargement des données)
3. Exécuter l'Exercice 2 (ingestion dans MinIO)
4. Exécuter l'Exercice 3 (création du Data Warehouse)
5. Lancer le Dashboard (Exercice 4)

**Durée estimée** : 10-20 minutes selon la connexion internet et la puissance de la machine.

### Option 2 : Exécuter un exercice spécifique

```bash
# Exercice 1 : Récupération des données depuis NYC TLC
./setup_and_run.sh ex01

# Exercice 2 : Ingestion des données dans MinIO
./setup_and_run.sh ex02

# Exercice 3 : Création des tables dans PostgreSQL
./setup_and_run.sh ex03

# Exercice 4 : Lancer le Dashboard Streamlit
./setup_and_run.sh ex04
```

> **Note** : Les exercices doivent être exécutés dans l'ordre (ex01 -> ex02 -> ex03 -> ex04) car chaque exercice dépend du précédent.

## Accès aux Services

Une fois les services démarrés :

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dashboard** | http://localhost:8501 | - |
| **Spark Master UI** | http://localhost:8081 | - |
| **MinIO Console** | http://localhost:9001 | Voir `.env` |
| **pgAdmin** | http://localhost:5050 | admin@admin.com / admin |

### Configuration pgAdmin

Pour vous connecter à PostgreSQL depuis pgAdmin :
- **Host** : `postgres-dw`
- **Port** : `5432`
- **Database** : `nyc_data_warehouse`
- **User** : `user_dw`
- **Password** : `password_dw`

## Description des Exercices

### Exercice 1 : Data Retrieval
Télécharge les fichiers Parquet des courses de taxi depuis le site NYC TLC et les stocke localement.

### Exercice 2 : Data Ingestion
- Lit les fichiers Parquet téléchargés
- Nettoie et transforme les données
- Détecte les outliers (courses anormales)
- Stocke les données nettoyées dans MinIO (format Parquet)

### Exercice 3 : SQL Table Creation
- Crée le schéma du Data Warehouse (modèle en étoile)
- Charge les données depuis MinIO vers PostgreSQL
- Crée les tables de faits et dimensions
- Génère les tables agrégées

### Exercice 4 : Dashboard
Dashboard interactif Streamlit avec :
- Vue d'ensemble et KPIs
- Analyse géographique
- Analyse temporelle
- Analyse des vendeurs et paiements
- Distributions des courses
- **Analyse détaillée des outliers** avec composition complète des prix

## Commandes Utiles

### Voir les logs d'un service

```bash
docker compose logs -f spark-master
docker compose logs -f dashboard
docker compose logs -f postgres-dw
```

### Arrêter tous les services

```bash
docker compose down
```

### Arrêter et supprimer les volumes (reset complet)

```bash
docker compose down -v
```

### Reconstruire les images

```bash
docker compose build --no-cache
```

### Vérifier l'état des services

```bash
docker compose ps
```

## Structure du Data Warehouse

### Dimensions
- `dim_date` : Calendrier
- `dim_location` : Zones géographiques NYC
- `dim_vendor` : Compagnies de taxi
- `dim_ratecode` : Codes tarifaires
- `dim_payment_type` : Types de paiement

### Tables de Faits
- `fact_trip` : Détail de chaque course
- `fact_vendor_daily` : Agrégation par vendeur et jour
- `fact_daily_pickup_zone` : Agrégation par zone de départ et jour
- `fact_daily_dropoff_zone` : Agrégation par zone d'arrivée et jour

## Dépannage

### Erreur "port already in use"
```bash
# Identifier le processus utilisant le port (ex: 5432)
sudo lsof -i :5432
# Ou arrêter tous les conteneurs Docker
docker stop $(docker ps -aq)
```

### Erreur de mémoire Spark
Augmentez la mémoire allouée à Docker (Settings > Resources > Memory).

### Le dashboard affiche "Database connection error"
Assurez-vous que l'exercice 3 a été exécuté avec succès :
```bash
./setup_and_run.sh ex03
```

### Réinitialiser complètement le projet
```bash
docker compose down -v
rm -rf minio-data postgres-data
./setup_and_run.sh all
```

---

## Référence : Code Spark avec MinIO

```scala
import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkApp extends App {
  val spark = SparkSession.builder()
    .appName("SparkApp")
    .master("local")
    .config("fs.s3a.access.key", "minio")
    .config("fs.s3a.secret.key", "minio123")
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "6000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
}
```

---

## Modalités de rendu

1. Pull Request vers la branch `master`
2. Dépôt du rapport et du code source zippé dans cours.cyu.fr

**Date limite de rendu : 7 février 2026**
