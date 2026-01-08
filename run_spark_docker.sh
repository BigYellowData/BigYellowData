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

echo "[INFO] Projet sbt : $PROJECT_DIR_ABS"
echo "[INFO] Classe principale : $MAIN_CLASS"

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

# ==============================================================================
# AJOUT SPÉCIFIQUE EXERCICE 3 : INITIALISATION SQL
# ==============================================================================
# On vérifie si creation.sql existe dans le dossier du projet
if [ -f "creation.sql" ] && [ -f "insertion.sql" ]; then
  echo "[INFO] 🛠  Détection des scripts SQL. Initialisation de la BDD..."

  # 1. Exécuter creation.sql (Recrée les tables)
  # L'option -i permet de passer le contenu du fichier via l'entrée standard (<)
  docker exec -i postgres-dw psql -U user_dw -d nyc_data_warehouse < creation.sql

  # 2. Exécuter insertion.sql (Données de référence)
  docker exec -i postgres-dw psql -U user_dw -d nyc_data_warehouse < insertion.sql

  echo "[INFO] ✅ Base de données initialisée."
else
  echo "[INFO] Pas de scripts SQL trouvés, on passe directement au job Spark."
fi
# ==============================================================================

# --- 6) Lancer spark-submit ---
echo "[INFO] Lancement de spark-submit..."

# J'ai ajouté les --packages et --conf nécessaires pour que l'exo 3 fonctionne
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --class "$MAIN_CLASS" \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.6.0 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/spark/"$JAR_NAME"

if [ -f "aggregation.sql" ]; then
  echo "[INFO] 📊 4. Calcul des Agrégats (Data Marts)..."
  docker exec -i postgres-dw psql -U user_dw -d nyc_data_warehouse < aggregation.sql
  echo "[INFO] ✅ Agrégations terminées."
else
  echo "[INFO] Pas de fichier aggregation.sql trouvé."
fi

echo "[INFO] 🎉 PIPELINE COMPLET TERMINÉ AVEC SUCCÈS !"