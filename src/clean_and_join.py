# -*- coding: utf-8 -*-
"""
clean_and_join.py — pré-nettoyage local (HDFS-ready)
Fusionne les fichiers de ventes, dédoublonne par transaction_id,
joint avec le catalogue produit, calcule le net_amount, et produit
un rapport statistique pour le reporting du projet UA2.

Sorties principales :
- outputs/clean/clean.csv         (fichier propre)
- outputs/rejects/rejects.csv     (fichier des anomalies)
- outputs/clean/stats_summary.csv
"""

from __future__ import annotations
import os, csv
from datetime import datetime
import pandas as pd
from typing import List
from common import parse_sales_rows, read_catalog

# === Définition des chemins ===
BASE = os.path.dirname(os.path.dirname(__file__))
DATA = os.path.join(BASE, "data")
OUT = os.path.join(BASE, "outputs")


def ensure_dirs():
    """Crée les dossiers nécessaires pour éviter les erreurs 'folder not found'."""
    for d in ["clean", "rejects", "metrics", "top10"]:
        os.makedirs(os.path.join(OUT, d), exist_ok=True)


def write_summary(path: str, clean_count: int, reject_count: int, unique_tx: int, files: List[str]):
    """Génère un résumé statistique du nettoyage (utile pour le rapport UA2)."""
    total = clean_count + reject_count
    valid_pct = (clean_count / total * 100) if total > 0 else 0
    reject_pct = (reject_count / total * 100) if total > 0 else 0
    summary_path = os.path.join(OUT, "clean", "stats_summary.csv")

    with open(summary_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date_execution", "fichiers_sources", "lignes_valides", "lignes_rejetees",
            "total_lignes", "transactions_uniques", "pct_valides", "pct_rejetees"
        ])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ", ".join(os.path.basename(f) for f in files),
            clean_count,
            reject_count,
            total,
            unique_tx,
            f"{valid_pct:.2f}%",
            f"{reject_pct:.2f}%"
        ])

    print(f"[stats]   -> {summary_path}")
    print(f"           ({valid_pct:.2f}% valides / {reject_pct:.2f}% rejetées)")


def main():
    ensure_dirs()

    # === Fichiers d'entrée ===
    in_files = [
        os.path.join(DATA, "ventes_multicanal.csv"),
        os.path.join(DATA, "ventes_increment_2025-10.csv"),
    ]
    cat_path = os.path.join(DATA, "catalogue_produits.csv")
    catalog = read_catalog(cat_path) if os.path.exists(cat_path) else pd.DataFrame()

    clean_path = os.path.join(OUT, "clean", "clean.csv")
    rej_path = os.path.join(OUT, "rejects", "rejects.csv")

    all_data = []
    n_bad = 0

    # === Lecture et nettoyage des fichiers sources ===
    for path in in_files:
        if not os.path.exists(path):
            print(f"[WARN] Fichier absent: {path}")
            n_bad += 1
            continue

        print(f"[INFO] Lecture de {os.path.basename(path)} ...")
        try:
            df_part = parse_sales_rows(path)
            all_data.append(df_part)
        except Exception as e:
            print(f"[ERROR] Erreur de lecture de {path}: {e}")
            n_bad += 1

    # === Fusion et dédoublonnage ===
    if not all_data:
        print("[ERROR] Aucun fichier valide à traiter.")
        return

    df = pd.concat(all_data, ignore_index=True)
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")

    # === Enrichissement avec le catalogue produit ===
    if catalog is not None and not catalog.empty:
        # Harmoniser les types avant la jointure
        df["product_id"] = df["product_id"].astype(str)
        catalog["product_id"] = catalog["product_id"].astype(str)

        df = df.merge(catalog[["product_id", "product_name", "category"]],
                      on="product_id", how="left")
    else:
        df["product_name"] = ""
        df["category"] = ""

    # === Calcul des champs dérivés ===
    if "date" in df.columns:
        df["yyyy_mm"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
    elif "ts" in df.columns:
        df["yyyy_mm"] = pd.to_datetime(df["ts"]).dt.strftime("%Y-%m")
    else:
        df["yyyy_mm"] = ""

    if "revenue" in df.columns:
        df["net_amount"] = df["revenue"]
    elif "unit_price" in df.columns and "qty" in df.columns:
        df["net_amount"] = df["unit_price"].astype(float) * df["qty"].astype(float)
    else:
        df["net_amount"] = 0

    # === Export du fichier nettoyé ===
    df.to_csv(clean_path, index=False)
    print(f"[clean]   -> {clean_path}")

    # === Export des rejets ===
    with open(rej_path, "w", encoding="utf-8", newline="") as f_bad:
        writer = csv.writer(f_bad)
        writer.writerow(["reason", "file"])
        if n_bad > 0:
            for f in in_files:
                if not os.path.exists(f):
                    writer.writerow(["missing_file", os.path.basename(f)])
    print(f"[rejects] -> {rej_path}")

    # === Statistiques de synthèse ===
    print("\n=== RÉSUMÉ DU NETTOYAGE ===")
    print(f"Fichiers traités : {', '.join(os.path.basename(f) for f in in_files)}")
    print(f"Lignes valides   : {len(df)}")
    print(f"Lignes rejetées  : {n_bad}")
    print(f"Transactions uniques : {df['transaction_id'].nunique()}")

    write_summary(clean_path, len(df), n_bad, df["transaction_id"].nunique(), in_files)
    print("============================\n")


if __name__ == "__main__":
    main()
