#!/usr/bin/env bash
set -euo pipefail

# --- 1) Vérif des arguments ---
if [ $# -lt 1 ]; then
  echo "Usage: $0 <sbt_project_dir> [MainClass]"
  echo "ex:   $0 ex03_warehouse IngestToDWH"
  exit 1
fi

PROJECT_DIR="$1"
MAIN_CLASS="${2:-SparkApp}"

# Chemin absolu
PROJECT_DIR_ABS="$(cd "$PROJECT_DIR" && pwd)"

echo "[INFO] Projet : $PROJECT_DIR_ABS"
echo "[INFO] Classe principale : $MAIN_CLASS"

# Vérifier si c'est un projet SQL pur (pas de build.sbt)
IS_SQL_ONLY=false
if [ ! -f "$PROJECT_DIR_ABS/build.sbt" ]; then
  echo "[INFO] 📋 Projet SQL pur détecté (pas de build.sbt)"
  IS_SQL_ONLY=true
  JAR_NAME=""
else
  # --- 2) Plugin sbt-assembly ---
  PLUGINS_DIR="$PROJECT_DIR_ABS/project"
  PLUGINS_FILE="$PLUGINS_DIR/plugins.sbt"
  mkdir -p "$PLUGINS_DIR"

  if [ ! -f "$PLUGINS_FILE" ] || ! grep -q "sbt-assembly" "$PLUGINS_FILE" 2>/dev/null; then
    echo "[INFO] Ajout du plugin sbt-assembly..."
    cat >> "$PLUGINS_FILE" <<'EOF'
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
EOF
  fi

  # --- 3) Build du fat JAR ---
  cd "$PROJECT_DIR_ABS"
  echo "[INFO] Compilation (sbt assembly)..."
  sbt -batch clean assembly

  # --- 4) Trouver le JAR ---
  JAR_PATH="$(find target -type f -name '*assembly*.jar' | sort | tail -n1 || true)"
  if [ -z "$JAR_PATH" ]; then
    echo "❌ Aucun JAR trouvé."
    exit 1
  fi
  JAR_NAME="$(basename "$JAR_PATH")"
  echo "[INFO] JAR trouvé : $JAR_NAME"

  # --- 5) Copie dans le conteneur ---
  echo "[INFO] Copie vers spark-master..."
  docker cp "$JAR_PATH" spark-master:/opt/spark/"$JAR_NAME"
fi

# ==============================================================================
# AJOUT SPÉCIFIQUE EXERCICE 3 : INITIALISATION SQL
# ==============================================================================
# On vérifie si creation.sql existe dans le dossier du projet
cd "$PROJECT_DIR_ABS"
if [ -f "creation.sql" ] && [ -f "insertion.sql" ]; then
  echo "[INFO] 🛠  Détection des scripts SQL. Initialisation de la BDD..."

  # Déterminer le nom du conteneur PostgreSQL
  POSTGRES_CONTAINER="bigyellowdata-postgres-dw-1"
  if ! docker ps | grep -q "$POSTGRES_CONTAINER"; then
    POSTGRES_CONTAINER="postgres-dw"
  fi
  echo "[INFO] Utilisation du conteneur PostgreSQL: $POSTGRES_CONTAINER"

  # 1. Exécuter creation.sql (Recrée les tables)
  echo "[INFO] 📋 Création du schéma et des tables..."
  docker exec -i "$POSTGRES_CONTAINER" psql -U user_dw -d nyc_data_warehouse < creation.sql

  if [ $? -ne 0 ]; then
    echo "❌ Erreur lors de l'exécution de creation.sql"
    exit 1
  fi

  # 2. Vérifier et charger le fichier taxi_zone_lookup.csv
  CSV_LOCAL_PATH="data/raw/taxi_zone_lookup.csv"
  if [ ! -f "$CSV_LOCAL_PATH" ]; then
    CSV_LOCAL_PATH="../data/raw/taxi_zone_lookup.csv"
  fi

  if [ ! -f "$CSV_LOCAL_PATH" ]; then
    echo "[INFO] 📥 Téléchargement de taxi_zone_lookup.csv..."
    mkdir -p data/raw 2>/dev/null || mkdir -p ../data/raw 2>/dev/null
    curl -sS -o "${CSV_LOCAL_PATH}" "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    if [ $? -ne 0 ]; then
      echo "⚠️  Impossible de télécharger taxi_zone_lookup.csv, continuez quand même..."
    else
      echo "[INFO] ✅ CSV téléchargé avec succès"
    fi
  fi

  # 3. Copier le CSV dans le conteneur PostgreSQL
  if [ -f "$CSV_LOCAL_PATH" ]; then
    echo "[INFO] 📦 Copie de taxi_zone_lookup.csv dans le conteneur..."
    docker cp "$CSV_LOCAL_PATH" "$POSTGRES_CONTAINER":/tmp/taxi_zone_lookup.csv
    if [ $? -eq 0 ]; then
      echo "[INFO] ✅ CSV copié dans le conteneur"
    fi
  fi

  # 4. Exécuter insertion.sql (Données de référence)
  echo "[INFO] 📊 Insertion des données de référence..."
  docker exec -i "$POSTGRES_CONTAINER" psql -U user_dw -d nyc_data_warehouse < insertion.sql

  if [ $? -eq 0 ]; then
    echo "[INFO] ✅ Base de données initialisée avec succès"
  else
    echo "❌ Erreur lors de l'insertion des données de référence"
    exit 1
  fi

  # Cleanup
  docker exec "$POSTGRES_CONTAINER" rm -f /tmp/taxi_zone_lookup.csv 2>/dev/null || true

else
  echo "[INFO] Pas de scripts SQL trouvés, on passe directement au job Spark."
fi
# ==============================================================================

# --- 6) Lancer spark-submit (seulement si pas SQL pur) ---
if [ "$IS_SQL_ONLY" = false ]; then
  echo "[INFO] Lancement de spark-submit..."

  # J'ai ajouté les --packages et --conf nécessaires pour que l'exo 3 fonctionne
  docker exec -i spark-master /opt/spark/bin/spark-submit \
    --class "$MAIN_CLASS" \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    /opt/spark/"$JAR_NAME"
else
  echo "[INFO] ⏭️  Pas de job Spark à exécuter (projet SQL pur)"
fi

# Utiliser le même nom de conteneur que défini plus haut
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-bigyellowdata-postgres-dw-1}"
if ! docker ps | grep -q "$POSTGRES_CONTAINER"; then
  POSTGRES_CONTAINER="postgres-dw"
fi

# Générer dim_date à partir des données réelles de fact_trip
if [ -f "populate_dim_date.sql" ]; then
  echo "[INFO] 📅 Génération de dim_date à partir des données réelles..."
  docker exec -i "$POSTGRES_CONTAINER" psql -U user_dw -d nyc_data_warehouse < populate_dim_date.sql
  if [ $? -eq 0 ]; then
    echo "[INFO] ✅ dim_date générée avec succès"
  else
    echo "⚠️  Erreur lors de la génération de dim_date"
  fi
fi

if [ -f "aggregation.sql" ]; then
  echo "[INFO] 📊 Calcul des Agrégats (Data Marts)..."

  docker exec -i "$POSTGRES_CONTAINER" psql -U user_dw -d nyc_data_warehouse < aggregation.sql

  if [ $? -eq 0 ]; then
    echo "[INFO] ✅ Agrégations terminées avec succès"
  else
    echo "⚠️  Erreur lors des agrégations (normal si fact_trip est vide)"
  fi
else
  echo "[INFO] Pas de fichier aggregation.sql trouvé."
fi

echo ""
echo "[INFO] 🎉 PIPELINE COMPLET TERMINÉ AVEC SUCCÈS !"