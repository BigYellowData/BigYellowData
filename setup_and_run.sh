#!/bin/bash
# ============================================================================
#  setup_and_run.sh
#  Script master pour démarrer l'infrastructure et exécuter les exercices
#
#  Usage: ./setup_and_run.sh [ex01|ex02|ex03|ex04|all]
# ============================================================================

set -e  # Exit on error

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}BigYellowData - Setup & Run${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Vérifier l'argument
if [ $# -lt 1 ]; then
  echo -e "${RED}Usage: $0 [ex01|ex02|ex03|ex04|all]${NC}"
  exit 1
fi

EXERCISE="$1"

# ==============================================================================
# 1. Démarrer l'infrastructure
# ==============================================================================
echo -e "${YELLOW}[1/4] 🚀 Démarrage de l'infrastructure Docker...${NC}"
docker compose up -d spark-master spark-worker-1 spark-worker-2 minio postgres-dw pgadmin

echo -e "${GREEN}✓ Conteneurs démarrés${NC}"
echo ""

# Attendre que les services soient prêts
echo -e "${YELLOW}[2/4] ⏳ Attente que les services soient prêts (15s)...${NC}"
sleep 15

# Vérifier PostgreSQL
until docker exec postgres-dw pg_isready -U user_dw -d nyc_data_warehouse > /dev/null 2>&1; do
  echo -e "${YELLOW}Attente de PostgreSQL...${NC}"
  sleep 2
done
echo -e "${GREEN}✓ PostgreSQL prêt${NC}"

# ==============================================================================
# 2. Initialiser MinIO
# ==============================================================================
echo -e "${YELLOW}[3/4] 🗄️  Initialisation de MinIO...${NC}"

# Charger les credentials depuis .env
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Créer le bucket s'il n'existe pas
docker exec minio mc alias set myminio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null || true
if ! docker exec minio mc ls myminio/nyctaxiproject > /dev/null 2>&1; then
  docker exec minio mc mb myminio/nyctaxiproject
  echo -e "${GREEN}✓ Bucket 'nyctaxiproject' créé${NC}"
else
  echo -e "${GREEN}✓ Bucket 'nyctaxiproject' existe déjà${NC}"
fi

# Télécharger taxi_zone_lookup.csv si absent (Ex01 l'uplodera vers MinIO)
if [ ! -f data/raw/taxi_zone_lookup.csv ]; then
  echo -e "${YELLOW}Téléchargement de taxi_zone_lookup.csv...${NC}"
  mkdir -p data/raw
  curl -sS -o data/raw/taxi_zone_lookup.csv "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
  echo -e "${GREEN}✓ taxi_zone_lookup.csv téléchargé${NC}"
else
  echo -e "${GREEN}✓ taxi_zone_lookup.csv existe déjà localement${NC}"
fi
echo -e "${YELLOW}(L'upload vers MinIO sera fait par Ex01)${NC}"

echo ""

# ==============================================================================
# 4. Exécuter les exercices
# ==============================================================================
echo -e "${YELLOW}[4/4] 📊 Exécution des exercices...${NC}"
echo ""

if [ "$EXERCISE" = "all" ]; then
  echo -e "${BLUE}=== Exercice 1: Data Retrieval ===${NC}"
  ./run_spark_docker.sh ex01_data_retrieval SparkApp
  echo ""

  echo -e "${BLUE}=== Exercice 2: Data Ingestion ===${NC}"
  ./run_spark_docker.sh ex02_data_ingestion SparkApp
  echo ""

  echo -e "${BLUE}=== Exercice 3: SQL Table Creation ===${NC}"
  ./run_spark_docker.sh ex03_sql_table_creation
  echo ""

  echo -e "${BLUE}=== Exercice 4: Dashboard Streamlit ===${NC}"
  docker compose up -d --build dashboard
  echo -e "${YELLOW}⏳ Attente du démarrage du dashboard...${NC}"
  until docker inspect --format='{{.State.Health.Status}}' dashboard_nyc 2>/dev/null | grep -q "healthy"; do
    sleep 2
  done
  echo -e "${GREEN}✓ Dashboard prêt !${NC}"
  echo ""

elif [ "$EXERCISE" = "ex01" ]; then
  echo -e "${BLUE}=== Exercice 1: Data Retrieval ===${NC}"
  ./run_spark_docker.sh ex01_data_retrieval SparkApp

elif [ "$EXERCISE" = "ex02" ]; then
  echo -e "${BLUE}=== Exercice 2: Data Ingestion ===${NC}"
  ./run_spark_docker.sh ex02_data_ingestion SparkApp

elif [ "$EXERCISE" = "ex03" ]; then
  echo -e "${BLUE}=== Exercice 3: SQL Table Creation ===${NC}"
  ./run_spark_docker.sh ex03_sql_table_creation

elif [ "$EXERCISE" = "ex04" ]; then
  echo -e "${BLUE}=== Exercice 4: Dashboard Streamlit ===${NC}"
  docker compose up -d --build dashboard
  echo -e "${YELLOW}⏳ Attente du démarrage du dashboard...${NC}"
  until docker inspect --format='{{.State.Health.Status}}' dashboard_nyc 2>/dev/null | grep -q "healthy"; do
    sleep 2
  done
  echo -e "${GREEN}✓ Dashboard prêt !${NC}"
  echo -e "${GREEN}🌐 Accéder au dashboard: http://localhost:8501${NC}"

else
  echo -e "${RED}Exercice inconnu: $EXERCISE${NC}"
  echo -e "${YELLOW}Usage: $0 [ex01|ex02|ex03|ex04|all]${NC}"
  exit 1
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✅ TERMINÉ AVEC SUCCÈS !${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}Services disponibles:${NC}"
echo "  - Spark Master UI:  http://localhost:8081"
echo "  - MinIO Console:    http://localhost:9001"
echo "  - pgAdmin:          http://localhost:5050"
if [ "$EXERCISE" = "ex04" ] || [ "$EXERCISE" = "all" ]; then
  echo "  - Dashboard:        http://localhost:8501"
fi
echo ""
