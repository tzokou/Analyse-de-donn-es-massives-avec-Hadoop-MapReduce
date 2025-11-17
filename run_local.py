# -*- coding: utf-8 -*-
"""
run_local.py — exécution locale du pipeline MapReduce (sans Hadoop)
Enchaîne: clean_and_join -> KPI pays/mois -> Top10 produits -> Return rate
Chaque job écrit lui-même ses fichiers de sortie dans /outputs.
"""
import os, sys, subprocess

BASE = os.path.dirname(__file__)
OUT  = os.path.join(BASE, "outputs")
NEEDED_DIRS = [
    os.path.join(OUT, "clean"),
    os.path.join(OUT, "metrics"),
    os.path.join(OUT, "top10"),
    os.path.join(OUT, "rejects"),
]

def sh(cmd: str):
    print("[RUN]", cmd)
    ec = subprocess.call(cmd, shell=True)
    if ec != 0:
        raise SystemExit(ec)

if __name__ == "__main__":
    # 0) Garantir l'existence des dossiers de sortie
    for d in NEEDED_DIRS:
        os.makedirs(d, exist_ok=True)

    # 1) Nettoyage / jointure
    sh(f"{sys.executable} src/clean_and_join.py")

    # 2) KPI ventes par pays/mois (le script écrit son propre CSV)
    sh(f"{sys.executable} -m src.job_kpi_sales_by_country_month outputs/clean/clean.csv")

    # 3) Top 10 produits (écrit son propre CSV)
    sh(f"{sys.executable} -m src.job_top10_products outputs/clean/clean.csv")

    # 4) Taux de retour global (écrit son JSONL)
    sh(f"{sys.executable} -m src.job_return_rate outputs/clean/clean.csv")

    print("[OK] Exécution locale terminée.")
