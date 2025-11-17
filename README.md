#   Pipeline Hadoop MapReduce (Ventes Multicanal)

**Cours :** Base de données massives avancées  
**Auteur :** Thierry Pascal Zokou Tchokonthe  
**Encadrement :** Collège La Cité – Hiver 2025  
**Langages :** Python 3 + `mrjob`  
**Cible :** Exécution locale & Cluster Hadoop Docker (`mzinee/hadoop-cluster:latest`) 

# Analyse-de-donn-es-massives-avec-Hadoop-MapReduce
Ce projet consiste à développer un pipeline complet de traitement de données volumineuses dans un environnement Hadoop MapReduce. L’objectif principal est d’automatiser le nettoyage, la fusion et l’agrégation de données issues de ventes multicanal afin de produire des indicateurs de performance exploitables à grande échelle.

---

## 1) Structure du projet
```
Projet_UA2_MapReduce/
│
├── data/                          # Données sources CSV
│   ├── ventes_multicanal.csv
│   ├── ventes_increment_2025-10.csv
│   └── catalogue_produits.csv
│
├── outputs/
│   ├── clean/                     # Données nettoyées
│   ├── rejects/                   # Lignes rejetées
│   ├── metrics/                   # KPI agrégés
│   └── top10/                     # Produits top 10
│
├── src/
│   ├── clean_and_join.py          # Pré-nettoyage & jointure
│   ├── common.py                  # Fonctions utilitaires
│   ├── job_kpi_sales_by_country_month.py
│   ├── job_top10_products.py
│   ├── job_return_rate.py
│   └── config.py
│
├── run_local.py                   # Exécution locale (inline)
├── run_hadoop.sh                  # Exécution cluster Hadoop
└── .env                           # Configuration des chemins et variables
```

---

## 2) Préparation de l’environnement

### Sur Windows (local)
```bash
conda activate hadoop_env
pip install pandas mrjob python-dotenv pytz
```

### Sur le cluster Hadoop
```bash
docker exec -it hadoop-master bash
source /root/.bashrc
start-hadoop.sh
```

---

## 3) Étape 1 — Nettoyage et préparation (`clean_and_join.py`)
Fusionne les ventes multicanal et incrémentales, dédoublonne les transactions, et joint le catalogue produits.

```bash
python src/clean_and_join.py
```

**Sorties :**
- `outputs/clean/clean.csv` — données propres  
- `outputs/rejects/rejects.csv` — anomalies détectées  
- `outputs/clean/stats_summary.csv` — statistiques d’exécution  

---

## 4) Étape 2 — Exécution Hadoop (MapReduce)

Lancer le pipeline complet depuis le conteneur maître :
```bash
cd /home/hduser/Projet_UA2_MapReduce
bash run_hadoop.sh
```

**Ce script effectue :**
1. Nettoyage et transfert de `clean.csv` vers HDFS `/tmp/ua2/clean.csv`
2. Exécution séquentielle des 3 jobs MapReduce :
   - `job_kpi_sales_by_country_month.py` → ventes par pays/mois  
   - `job_top10_products.py` → top 10 produits  
   - `job_return_rate.py` → taux de retour global
3. Stockage des résultats dans :
   ```
   /home/hduser/Projet_UA2_MapReduce/outputs/
       ├── metrics/sales_by_country_month.csv
       ├── metrics/return_rate.jsonl
       └── top10/top10_products.csv
   ```

---

## 5) Étape 3 — Exécution locale (optionnelle)
Permet de valider le pipeline sans cluster Hadoop :
```bash
python run_local.py
```

**Résultats :**
- Nettoyage identique à Hadoop (5104 lignes valides / 0 rejet)
- Production locale des mêmes fichiers de sortie (`outputs/clean/`, `outputs/metrics/`, `outputs/top10/`)

---

## 6) KPI calculés
| Indicateur | Description | Formule |
|-------------|--------------|----------|
| **CA net (ligne)** | Valeur nette par transaction | `qty * unit_price - coupon_value + shipping_cost` |
| **Ventes nettes par pays/mois** | Somme du CA net par `(country, yyyy-mm)` | `Σ(net_amount)` |
| **Top 10 produits** | Produits les plus vendus globalement | `Top 10 (Σ(qty))` |
| **Taux de retour global** | Rapport des ventes retournées | `|Σ(qty<0)| / Σ(qty>0)` |

---

## 7) Gestion des rejets
- Les enregistrements invalides (`product_id` vide, `date` incorrecte, etc.) sont consignés dans `outputs/rejects/rejects.csv`.  
- Exemple Hadoop :  
  ```
  parse_error:empty product_id:line=37,ventes_multicanal.csv
  parse_error:invalid ts/date value: ts:line=174,ventes_multicanal.csv
  ```

---

## 8) Résumé d’exécution
| Critère | Exécution locale | Exécution Hadoop |
|----------|------------------|------------------|
| Lignes traitées | 5104 | 5261 |
| Lignes rejetées | 0 | 3 |
| Environnement | `mrjob (inline)` | Hadoop/YARN |
| Fichier source | CSV local | HDFS `/tmp/ua2/clean.csv` |
| Temps d’exécution | ~12 s | ~45 s |
| Résultats | `outputs/` | `/home/hduser/Projet_UA2_MapReduce/outputs/` |

---

## 9) Bonnes pratiques
- Toujours exécuter `start-hadoop.sh` avant `run_hadoop.sh`.  
- Vérifier les permissions de `/tmp/ua2/` sur HDFS.  
- Nettoyer les anciens jobs :  
  ```bash
  hdfs dfs -rm -r /tmp/ua2
  ```

---

