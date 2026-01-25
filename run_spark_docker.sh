#!/usr/bin/env bash
set -euo pipefail

# --- 1) Vérif des arguments ---
if [ $# -lt 1 ]; then
  echo "Usage: $0 <sbt_project_dir> [MainClass]"
  echo "ex:   $0 ex02_data_ingestion SparkApp"
  exit 1
fi

PROJECT_DIR="$1"
MAIN_CLASS="${2:-SparkApp}"   # SparkApp par défaut si non fourni

# On passe en chemin absolu pour éviter les surprises
PROJECT_DIR_ABS="$(cd "$PROJECT_DIR" && pwd)"

echo "[INFO] Projet sbt : $PROJECT_DIR_ABS"
echo "[INFO] Classe principale : $MAIN_CLASS"

# --- 2) Ajouter le plugin sbt-assembly si besoin ---
PLUGINS_DIR="$PROJECT_DIR_ABS/project"
PLUGINS_FILE="$PLUGINS_DIR/plugins.sbt"

mkdir -p "$PLUGINS_DIR"

if [ ! -f "$PLUGINS_FILE" ] || ! grep -q "sbt-assembly" "$PLUGINS_FILE" 2>/dev/null; then
  echo "[INFO] Ajout du plugin sbt-assembly dans $PLUGINS_FILE"
  cat >> "$PLUGINS_FILE" <<'EOF'
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
EOF
else
  echo "[INFO] sbt-assembly déjà présent dans $PLUGINS_FILE"
fi

# --- 3) Build du fat JAR ---
cd "$PROJECT_DIR_ABS"
echo "[INFO] Lancement de 'sbt clean assembly'..."
sbt -batch clean assembly

# --- 4) Trouver le JAR assembly généré ---
JAR_PATH="$(find target -type f -name '*assembly*.jar' | sort | tail -n1 || true)"

if [ -z "$JAR_PATH" ]; then
  echo "❌ Aucun JAR *assembly* trouvé dans target/. Vérifie ton build.sbt."
  exit 1
fi

JAR_NAME="$(basename "$JAR_PATH")"
echo "[INFO] JAR trouvé : $JAR_PATH (nom = $JAR_NAME)"

# --- 5) Copier le JAR dans le conteneur spark-master ---
echo "[INFO] Copie du JAR dans le conteneur spark-master..."
docker cp "$JAR_PATH" spark-master:/opt/spark/"$JAR_NAME"

# --- 6) Lancer spark-submit dans Docker ---
echo "[INFO] Lancement de spark-submit dans spark-master..."
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --class "$MAIN_CLASS" \
  --master spark://spark-master:7077 \
  /opt/spark/"$JAR_NAME"
