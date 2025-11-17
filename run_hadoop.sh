#!/usr/bin/env bash
# ============================================================
#  Script pipeline UA2 - Hadoop MapReduce automatisé
#  Objet : Automatiser le nettoyage, le dépôt HDFS, le traitement MapReduce et la récupération des résultats pour un pipeline analytique de ventes multicanal.
#  Prérequis :
#    - Docker Hadoop opérationnel (cluster type mzinee/hadoop-cluster)
#    - Fichier .env correctement paramétré (variables chemins et jobs)
#    - Tous les scripts Python MapReduce sont dans le répertoire src/
# ============================================================

set -Eeuo pipefail

echo "=== [UA2] Pipeline Hadoop MapReduce ==="

# 1️⃣ Charger le fichier .env s'il existe (sinon, arrêt avec message clair)
if [ -f ".env" ]; then
  set -a                   
  source .env
  set +a
else
  echo "[ERREUR] Fichier .env introuvable. Arrêt du script."
  exit 1
fi

# 2️⃣ Vérification des variables essentielles
REQUIRED_VARS=(RAW_DIR RAW_SALES RAW_SALES_INCREMENT RAW_PRODUCTS HDFS_ROOT HDFS_INPUT HDFS_CLEAN HDFS_OUTPUT)
for var in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!var:-}" ]; then
    echo "[ERREUR] Variable d'environnement manquante : $var"
    exit 1
  fi
done

PY_BIN="${PY_BIN:-python3}"

# === Étape 1 : Nettoyage local
echo "=== [1/4] Nettoyage et préparation locale ==="
$PY_BIN src/clean_and_join.py || { echo "[ERREUR] Échec du nettoyage local."; exit 1; }

# === Étape 2 : Publication sur HDFS
echo "=== [2/4] Publication du fichier clean.csv sur HDFS ==="
hdfs dfs -rm -r -f "$HDFS_ROOT" || true
hdfs dfs -mkdir -p "$HDFS_INPUT" "$HDFS_CLEAN" "$HDFS_OUTPUT"

if [ ! -f "$CLEAN_DIR/clean.csv" ]; then
  echo "[ERREUR] Le fichier $CLEAN_DIR/clean.csv est introuvable. Arrêt du script."
  exit 1
fi

hdfs dfs -put -f "$CLEAN_DIR/clean.csv" "$HDFS_CLEAN/clean.csv"
echo "✅ Fichier transféré sur HDFS : $HDFS_CLEAN/clean.csv"

# === Étape 3 : Lancement des jobs MapReduce dans src/ ===
echo "=== [3/4] Exécution des jobs MapReduce ==="
echo "→ Job 1 : KPI Country-Month"
$PY_BIN src/job_kpi_sales_by_country_month.py -r hadoop "$HDFS_CLEAN/clean.csv" \
  --output-dir "$HDFS_OUTPUT/kpi_country_month" --no-output

echo "→ Job 2 : Top $TOPN produits"
$PY_BIN src/job_top10_products.py -r hadoop "$HDFS_CLEAN/clean.csv" \
  --output-dir "$HDFS_OUTPUT/top_products" --no-output

echo "→ Job 3 : Taux de retour"
$PY_BIN src/job_return_rate.py -r hadoop "$HDFS_CLEAN/clean.csv" \
  --output-dir "$HDFS_OUTPUT/return_rate" --no-output

# === Étape 4 : Rapatriement local des fichiers résultats ===
echo "=== [4/4] Récupération des résultats ==="
mkdir -p "$WORK_DIR/results"

hdfs dfs -get -f "$HDFS_OUTPUT/top_products/part-*" "$WORK_DIR/results/top_products.csv" || echo "[WARN] Résultat top_products manquant."
hdfs dfs -get -f "$HDFS_OUTPUT/kpi_country_month/part-*" "$WORK_DIR/results/kpi_country_month.csv" || echo "[WARN] Résultat KPI manquant."
hdfs dfs -get -f "$HDFS_OUTPUT/return_rate/part-*" "$WORK_DIR/results/return_rate.jsonl" || echo "[WARN] Résultat return_rate manquant."

echo ""
echo "✅ Pipeline Hadoop MapReduce terminé avec succès."
echo "📂 Résultats locaux : $WORK_DIR/results/"
echo "📁 Données HDFS     : $HDFS_CLEAN/clean.csv et $HDFS_OUTPUT/"
echo "==============================================================="

