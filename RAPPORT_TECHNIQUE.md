# Rapport Technique - BigYellowData

## Pipeline de Data Engineering pour l'Analyse des Taxis Jaunes de New York

---

## Table des Matières

1. [Introduction](#1-introduction)
2. [Architecture Technique](#2-architecture-technique)
3. [Infrastructure et Services](#3-infrastructure-et-services)
4. [Description des Exercices](#4-description-des-exercices)
5. [Modélisation des Données](#5-modélisation-des-données)
6. [Analyse des Données](#6-analyse-des-données)
7. [Machine Learning](#7-machine-learning)
8. [Orchestration et Automatisation](#8-orchestration-et-automatisation)
9. [Conclusion](#9-conclusion)

---

## 1. Introduction

### 1.1 Contexte du Projet

Le projet **BigYellowData** est une solution complète de data engineering qui implémente un pipeline ETL (Extract, Transform, Load) de bout en bout pour l'analyse des données des taxis jaunes de New York City. Ces données, fournies par la NYC Taxi and Limousine Commission (TLC), représentent des millions de courses de taxi avec des informations détaillées sur les trajets, les tarifs et les modes de paiement.

### 1.2 Objectifs

- Construire un **Data Lake** pour le stockage des données brutes
- Implémenter un pipeline de **nettoyage et transformation** des données
- Concevoir un **Data Warehouse** optimisé pour l'analyse OLAP
- Développer un **dashboard interactif** pour la visualisation
- Créer un **service de prédiction ML** pour estimer les prix des courses
- Mettre en place une **orchestration automatisée** du pipeline

### 1.3 Stack Technologique

| Couche | Technologies |
|--------|--------------|
| Traitement Big Data | Apache Spark 3.5.5, Scala 2.13 |
| Data Lake | MinIO (compatible S3), format Parquet |
| Data Warehouse | PostgreSQL 15 |
| Visualisation | Streamlit, Plotly |
| Machine Learning | scikit-learn, Random Forest |
| Orchestration | Apache Airflow |
| Conteneurisation | Docker, Docker Compose |

---

## 2. Architecture Technique

### 2.1 Vue d'Ensemble

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BIGYELOWDATA ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│   │   NYC TLC    │     │    MinIO     │     │  PostgreSQL  │                │
│   │  Data Source │────▶│  Data Lake   │────▶│     DWH      │                │
│   └──────────────┘     │   (S3)       │     │              │                │
│         │              └──────────────┘     └──────────────┘                │
│         │                     │                    │                         │
│         ▼                     ▼                    ▼                         │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│   │    Ex01      │     │    Ex02      │     │    Ex03      │                │
│   │  Retrieval   │────▶│  Ingestion   │────▶│  SQL Tables  │                │
│   │   (Spark)    │     │   (Spark)    │     │   (SQL)      │                │
│   └──────────────┘     └──────────────┘     └──────────────┘                │
│                                                    │                         │
│                        ┌───────────────────────────┼───────────────┐        │
│                        ▼                           ▼               ▼        │
│                  ┌──────────────┐           ┌──────────────┐ ┌──────────┐   │
│                  │    Ex04      │           │    Ex05      │ │  Ex06    │   │
│                  │  Dashboard   │           │  ML Service  │ │ Airflow  │   │
│                  │ (Streamlit)  │           │  (Python)    │ │          │   │
│                  └──────────────┘           └──────────────┘ └──────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Flux de Données

Le flux de données suit une architecture en couches :

1. **Couche Bronze (Raw)** : Données brutes en format Parquet stockées dans MinIO (`nyc_raw/`)
2. **Couche Silver (Refined)** : Données nettoyées et enrichies (`dwh/yellow_taxi_refined/`)
3. **Couche Gold (Aggregated)** : Tables agrégées dans PostgreSQL pour l'analyse

### 2.3 Schéma de Réseau

Tous les services communiquent via un réseau Docker bridge nommé `spark-network`. Les ports exposés sont :

| Service | Port | Description |
|---------|------|-------------|
| Spark Master UI | 8081 | Interface web Spark |
| Spark Master | 7077 | Communication inter-nodes |
| MinIO API | 9000 | API S3 compatible |
| MinIO Console | 9001 | Interface d'administration |
| PostgreSQL | 5432 | Base de données |
| pgAdmin | 5050 | Administration PostgreSQL |
| Dashboard | 8501 | Application Streamlit |
| Airflow | 8082 | Interface Airflow |

---

## 3. Infrastructure et Services

### 3.1 Cluster Apache Spark

Le cluster Spark est composé de 3 conteneurs :

```yaml
spark-master:
  - Rôle: Coordinateur du cluster
  - Mémoire: Variable selon la machine hôte
  - Ports: 7077 (RPC), 8081 (Web UI)

spark-worker-1 & spark-worker-2:
  - Rôle: Exécution des tâches
  - Mémoire: 2 GB chacun
  - Cores: 2 par worker
```

**Configuration optimisée** :
- Adaptive Query Execution (AQE) activé
- Auto-broadcast join threshold: 50 MB
- Compression Parquet: Snappy
- Sérialisation: Kryo

### 3.2 MinIO (Data Lake)

MinIO fournit un stockage objet compatible S3 :

- **Bucket principal** : `nyctaxiproject`
- **Structure** :
  ```
  nyctaxiproject/
  ├── nyc_raw/                    # Données brutes (Ex01)
  │   └── *.parquet
  ├── taxi_zone_lookup.csv        # Référentiel des zones
  └── dwh/
      └── yellow_taxi_refined/    # Données nettoyées (Ex02)
          └── date_id=*/
              └── *.parquet
  ```

### 3.3 PostgreSQL (Data Warehouse)

Base de données `nyc_data_warehouse` avec schéma `dw` :

- **Utilisateur** : `user_dw`
- **Modèle** : Schéma en constellation
- **Tables** : 9 (5 dimensions + 4 faits)

### 3.4 Apache Airflow

Orchestration avec 3 composants :

- **airflow-postgres** : Métadonnées Airflow
- **airflow-webserver** : Interface utilisateur (port 8082)
- **airflow-scheduler** : Planification des DAGs

---

## 4. Description des Exercices

### 4.1 Exercice 1 : Récupération des Données

**Objectif** : Télécharger les données et les stocker dans le Data Lake

**Implémentation** (Scala/Spark) :

```scala
// Lecture des fichiers Parquet locaux
val df = spark.read.parquet("/data/raw/*.parquet")

// Upload vers MinIO
df.write.mode("overwrite").parquet(s"s3a://${bucketName}/nyc_raw/")
```

**Données transférées** :
- Fichiers Parquet des courses de taxi
- Fichier CSV de référence des zones (265 zones)

### 4.2 Exercice 2 : Ingestion et Nettoyage

**Objectif** : Nettoyer, valider et transformer les données brutes

**Pipeline en 6 étapes** :

| Étape | Description | Critères |
|-------|-------------|----------|
| 1 | Chargement zones | 265 zones TLC |
| 2 | Filtrage contractuel | Passagers 1-6, Année >= 2020, IDs valides |
| 3 | Métriques dérivées | Durée, vitesse, ratio pourboire |
| 4 | Détection remboursements | Paires course/annulation |
| 5 | Détection outliers | IQR, Z-Score, règles métier |
| 6 | Écriture partitionnée | Par date_id |

**Critères de détection des outliers** :

```scala
val MIN_DURATION_MINUTES = 2.0
val MAX_SPEED_MPH = 120.0
val MAX_PRICE_PER_MILE = 25.0
val MAX_DURATION_HOURS = 5.0
val ZSCORE_THRESHOLD = 3.0
```

**Flags de qualité** :
- `is_outlier` : Boolean indiquant une anomalie
- `outlier_reason` : "High Speed", "Price/Mile > $25", "Duration > 5h"
- `data_quality_score` : 0 (outlier), 75 (acceptable), 100 (parfait)

### 4.3 Exercice 3 : Création du Data Warehouse

**Objectif** : Construire un entrepôt de données OLAP

**Schéma en constellation** :

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │   (calendrier)  │
                    └────────┬────────┘
                             │
    ┌──────────────┐         │         ┌──────────────┐
    │ dim_vendor   │◄────────┼────────►│ dim_location │
    └──────────────┘         │         └──────────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
    ┌──────────────────┐         ┌──────────────────┐
    │   fact_trip      │         │ fact_vendor_daily│
    │  (grain: course) │         │ (grain: jour)    │
    └──────────────────┘         └──────────────────┘
              │
              ├─────────────────────────────┐
              ▼                             ▼
    ┌──────────────────┐         ┌──────────────────┐
    │fact_daily_pickup │         │fact_daily_dropoff│
    │     _zone        │         │     _zone        │
    └──────────────────┘         └──────────────────┘
```

**Tables de dimensions** :

| Table | Clé Primaire | Colonnes |
|-------|--------------|----------|
| dim_date | date_id | full_date, year, month, day, day_of_week, is_weekend |
| dim_location | location_id | borough, zone, service_zone |
| dim_vendor | vendor_id | vendor_name |
| dim_ratecode | ratecode_id | description |
| dim_payment_type | payment_type_id | description |

**Table de faits principale (fact_trip)** :

| Catégorie | Colonnes |
|-----------|----------|
| Clés étrangères | date_id, vendor_id, ratecode_id, payment_type_id, pickup_location_id, dropoff_location_id |
| Timestamps | tpep_pickup_datetime, tpep_dropoff_datetime |
| Mesures | trip_distance, fare_amount, tip_amount, total_amount, ... |
| Dérivées | trip_duration_minutes, avg_speed_mph, tip_ratio |
| Qualité | is_outlier, outlier_reason |

### 4.4 Exercice 4 : Dashboard Analytique

**Objectif** : Visualisation interactive des données

**Technologies** : Streamlit, Plotly, SQLAlchemy

**6 onglets implémentés** :

1. **Vue d'ensemble**
   - KPIs : Total courses, revenu, tarif moyen, distance, durée
   - Tendances journalières
   - Score de qualité des données

2. **Analyse Géographique**
   - Top 10 zones de pickup/dropoff
   - Répartition par arrondissement
   - Trajets les plus fréquents

3. **Analyse Temporelle**
   - Distribution horaire des courses
   - Variations par jour de la semaine
   - Évolution des tarifs

4. **Vendeurs & Paiements**
   - Part de marché par vendeur
   - Types de paiement utilisés
   - Codes tarifaires

5. **Distributions**
   - Tranches de distance
   - Tranches de tarif
   - Nombre de passagers

6. **Analyse des Outliers**
   - Comparaison outliers vs normal
   - Patterns temporels des anomalies
   - Échantillons détaillés avec export CSV

### 4.5 Exercice 5 : Service de Prédiction ML

**Objectif** : Prédire le prix d'une course de taxi

**Architecture modulaire** :

```
ex05_ml_prediction_service/
├── src/
│   ├── train.py          # Pipeline d'entraînement
│   ├── inference.py      # Moteur de prédiction
│   ├── data_manager.py   # ETL pour ML
│   ├── model_manager.py  # Persistance modèle
│   └── app.py            # Interface Streamlit
├── models/
│   └── taxi_price_model.joblib
└── tests/
    ├── test_train.py
    └── test_inference.py
```

**Features utilisées** :

| Feature | Description | Type |
|---------|-------------|------|
| trip_distance | Distance du trajet | Numérique |
| pickup_location_id | Zone de départ | Catégoriel |
| dropoff_location_id | Zone d'arrivée | Catégoriel |
| pickup_hour | Heure de prise en charge | Numérique |
| day_of_week | Jour de la semaine | Numérique |
| is_rush_hour | Heure de pointe (7-9h, 17-19h) | Binaire |
| is_weekend | Week-end | Binaire |
| is_night | Nuit (22h-5h) | Binaire |
| is_airport_trip | Trajet aéroport | Binaire |

**Modèle** : Random Forest Regressor
- 100 estimateurs
- Profondeur maximale : 15
- Min samples split : 10

**Garde-fou** : Tests unitaires exécutés avant chaque entraînement

### 4.6 Exercice 6 : Orchestration Airflow

**Objectif** : Automatiser le pipeline complet

**DAG 1 : `nyc_taxi_full_pipeline`** (manuel)

```
start → check_infrastructure → ex01_data_retrieval → ex02_data_ingestion
                                                           │
                                                           ▼
                                                    ex03_dwh_loading
                                                    ├── create_schema
                                                    ├── copy_taxi_zones
                                                    ├── insert_reference_data
                                                    ├── load_fact_table
                                                    ├── populate_dim_date
                                                    └── run_aggregations
                                                           │
                                                           ▼
                                                    ex05_ml_training → end
```

**DAG 2 : `nyc_taxi_monthly_refresh`** (planifié)

- **Schedule** : `0 2 1 * *` (1er du mois à 2h00)
- **Actions** : Rafraîchissement données + Ré-entraînement ML

---

## 5. Modélisation des Données

### 5.1 Données Sources

Les données NYC TLC Yellow Taxi contiennent les colonnes suivantes :

| Colonne | Type | Description |
|---------|------|-------------|
| VendorID | Integer | Fournisseur TPEP (1=CMT, 2=VeriFone) |
| tpep_pickup_datetime | Timestamp | Date/heure de prise en charge |
| tpep_dropoff_datetime | Timestamp | Date/heure de dépose |
| passenger_count | Integer | Nombre de passagers |
| trip_distance | Double | Distance en miles |
| RatecodeID | Integer | Code tarifaire |
| store_and_fwd_flag | String | Y=stocké avant envoi |
| PULocationID | Integer | Zone de pickup (1-265) |
| DOLocationID | Integer | Zone de dropoff (1-265) |
| payment_type | Integer | Mode de paiement |
| fare_amount | Double | Tarif de base |
| extra | Double | Suppléments |
| mta_tax | Double | Taxe MTA (0.50$) |
| tip_amount | Double | Pourboire |
| tolls_amount | Double | Péages |
| improvement_surcharge | Double | Taxe amélioration |
| congestion_surcharge | Double | Taxe congestion |
| airport_fee | Double | Frais aéroport |
| total_amount | Double | Montant total |

### 5.2 Référentiel des Zones

Le fichier `taxi_zone_lookup.csv` contient 265 zones réparties en 6 boroughs :

| Borough | Nombre de zones |
|---------|-----------------|
| Manhattan | 69 |
| Queens | 69 |
| Brooklyn | 61 |
| Bronx | 43 |
| Staten Island | 20 |
| EWR (Newark) | 1 |
| Unknown | 2 |

### 5.3 Transformations Appliquées

**Colonnes dérivées ajoutées par Ex02** :

| Colonne | Formule |
|---------|---------|
| trip_duration_hours | (dropoff - pickup) / 3600 |
| avg_speed_mph | trip_distance / trip_duration_hours |
| tip_ratio | tip_amount / total_amount |
| price_per_mile | total_amount / trip_distance |
| date_id | Format YYYYMMDD |

---

## 6. Analyse des Données

### 6.1 Volumétrie

Les données traitées représentent typiquement :

| Métrique | Valeur approximative |
|----------|----------------------|
| Période couverte | 2020 - présent |
| Nombre de courses | 5-10 millions |
| Taille brute | ~200 MB (Parquet) |
| Après nettoyage | ~180 MB |

### 6.2 Règles de Qualité

Les données sont filtrées selon ces critères :

| Règle | Critère |
|-------|---------|
| Passagers valides | 1 ≤ passenger_count ≤ 6 |
| Distance minimale | trip_distance ≥ 0.3 miles |
| Durée minimale | duration ≥ 2 minutes |
| Vitesse maximale | speed ≤ 120 mph |
| Prix cohérent | total_amount ≥ 0 |
| Zones valides | PU/DO LocationID ∈ [1-265] |

### 6.3 Détection des Outliers

**Méthode IQR (Interquartile Range)** :

```
Limite basse = Q1 - 1.5 × IQR
Limite haute = Q3 + 1.5 × IQR
```

Appliquée sur : `trip_distance`, `total_amount`, `avg_speed_mph`, `trip_duration_hours`

**Méthode Z-Score par route** :

Pour chaque paire (pickup, dropoff) avec au moins 10 courses :
```
Z = |distance - moyenne_route| / écart_type_route
Outlier si Z > 3
```

### 6.4 Statistiques Observées

Les données nettoyées présentent typiquement :

| Métrique | Valeur moyenne |
|----------|----------------|
| Distance | 3-5 miles |
| Durée | 15-20 minutes |
| Tarif total | $15-25 |
| Pourboire (CB) | 15-20% |
| Taux d'outliers | 1-3% |

### 6.5 Patterns Temporels

**Distribution horaire** :
- Pics : 8-9h (rush matin), 18-19h (rush soir)
- Creux : 3-5h (nuit)

**Distribution hebdomadaire** :
- Plus d'activité : Vendredi, Samedi
- Moins d'activité : Dimanche matin

---

## 7. Machine Learning

### 7.1 Problématique

Prédire le prix total d'une course de taxi (`total_amount`) à partir des caractéristiques du trajet.

### 7.2 Feature Engineering

**Features temporelles** :

```python
df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek
df['is_rush_hour'] = df['pickup_hour'].isin([7, 8, 9, 17, 18, 19])
df['is_weekend'] = df['day_of_week'] >= 5
df['is_night'] = df['pickup_hour'].isin([22, 23, 0, 1, 2, 3, 4, 5])
```

**Feature aéroport** :

```python
airport_ids = [1, 132, 138]  # Newark, JFK, LaGuardia
df['is_airport_trip'] = (
    df['pickup_location_id'].isin(airport_ids) |
    df['dropoff_location_id'].isin(airport_ids)
)
```

### 7.3 Modèle Choisi

**Random Forest Regressor** avec les hyperparamètres :

| Paramètre | Valeur | Justification |
|-----------|--------|---------------|
| n_estimators | 100 | Bon compromis performance/temps |
| max_depth | 15 | Évite le surapprentissage |
| min_samples_split | 10 | Régularisation |
| n_jobs | -1 | Parallélisation maximale |

### 7.4 Pipeline d'Entraînement

```
1. Téléchargement depuis MinIO
         ↓
2. Split Train/Test (80/20)
         ↓
3. Filtrage des outliers
         ↓
4. Feature engineering
         ↓
5. Entraînement Random Forest
         ↓
6. Évaluation (RMSE)
         ↓
7. Sauvegarde (joblib)
```

### 7.5 Métriques d'Évaluation

- **RMSE** (Root Mean Square Error) : Métrique principale
- Objectif : RMSE < $5 pour une prédiction exploitable

---

## 8. Orchestration et Automatisation

### 8.1 Architecture Airflow

```
┌─────────────────────────────────────────────────────────────┐
│                       AIRFLOW CLUSTER                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────┐    ┌─────────────────┐                 │
│  │ airflow-postgres│    │ airflow-init    │                 │
│  │  (Metadata DB)  │◄───│ (DB Setup)      │                 │
│  └────────┬────────┘    └─────────────────┘                 │
│           │                                                  │
│           ▼                                                  │
│  ┌─────────────────┐    ┌─────────────────┐                 │
│  │airflow-webserver│◄──►│airflow-scheduler│                 │
│  │   (Port 8082)   │    │                 │                 │
│  └─────────────────┘    └─────────────────┘                 │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 DAGs Implémentés

**DAG 1 : Pipeline Complet (Manuel)**

| Task ID | Description | Dépendances |
|---------|-------------|-------------|
| start | Début du workflow | - |
| check_infrastructure | Vérifie les conteneurs | start |
| ex01_data_retrieval | Téléchargement données | check_infrastructure |
| ex02_data_ingestion | Nettoyage Spark | ex01 |
| ex03_dwh_loading | Chargement DWH (6 sous-tâches) | ex02 |
| ex05_ml_training | Entraînement modèle | ex03 |
| end | Fin du workflow | ex05 |

**DAG 2 : Rafraîchissement Mensuel (Planifié)**

- **Cron** : `0 2 1 * *`
- **Retry** : 2 tentatives, délai 2 minutes
- **Actions** : Même séquence que DAG 1

### 8.3 Gestion des Erreurs

- Retry automatique (2 tentatives)
- Logs centralisés dans `/opt/airflow/logs`
- Alertes configurable par email

---

## 9. Conclusion

### 9.1 Résumé des Réalisations

Le projet BigYellowData implémente avec succès :

1. **Pipeline ETL robuste** : Extraction, transformation et chargement automatisés
2. **Qualité des données** : Détection d'outliers multicritères (IQR, Z-Score, règles métier)
3. **Data Warehouse** : Schéma en constellation optimisé pour l'analyse OLAP
4. **Visualisation** : Dashboard interactif avec 6 vues analytiques
5. **Machine Learning** : Prédiction de prix avec Random Forest
6. **Orchestration** : Automatisation complète via Airflow

### 9.2 Points Forts Techniques

- **Scalabilité** : Architecture Spark distribuée
- **Reproductibilité** : Conteneurisation Docker complète
- **Maintenabilité** : Code modulaire et documenté
- **Qualité** : Tests unitaires et garde-fous

### 9.3 Évolutions Possibles

| Amélioration | Description |
|--------------|-------------|
| Streaming | Intégration Kafka pour données temps réel |
| ML avancé | XGBoost, réseaux de neurones |
| Géospatial | Visualisation carte interactive |
| Monitoring | Prometheus + Grafana pour les métriques |
| CI/CD | Pipeline GitHub Actions |

---

## Annexes

### A. Commandes Utiles

```bash
# Démarrer l'infrastructure
docker-compose up -d

# Exécuter le pipeline complet
./setup_and_run.sh all

# Accéder aux interfaces
# Spark UI:      http://localhost:8081
# MinIO:         http://localhost:9001
# pgAdmin:       http://localhost:5050
# Dashboard:     http://localhost:8501
# Airflow:       http://localhost:8082
```

### B. Identifiants par Défaut

| Service | Utilisateur | Mot de passe |
|---------|-------------|--------------|
| MinIO | minioadmin | minioadmin |
| PostgreSQL | user_dw | password_dw |
| pgAdmin | admin@admin.com | admin |
| Airflow | admin | admin |

### C. Structure des Répertoires

```
BigYellowData/
├── data/
│   ├── raw/              # Données brutes
│   ├── processed/        # Données ML
│   └── external/         # Données externes
├── ex01_data_retrieval/  # Exercice 1
├── ex02_data_ingestion/  # Exercice 2
├── ex03_sql_table_creation/ # Exercice 3
├── ex04_dashboard/       # Exercice 4
├── ex05_ml_prediction_service/ # Exercice 5
├── ex06_airflow/         # Exercice 6
├── docker/               # Configuration Spark
├── docker-compose.yml    # Orchestration services
└── setup_and_run.sh      # Script principal
```

---

*Document généré le : Février 2026*
*Version du projet : 1.0*
