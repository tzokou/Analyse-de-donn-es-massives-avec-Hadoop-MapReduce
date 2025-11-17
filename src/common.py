"""
common.py – Fonctions utilitaires partagées entre les scripts du pipeline MapReduce UA2
Auteur : Thierry Pascal Zokou
Projet : UA2 – Hadoop MapReduce (ventes multicanal)

But :
- Offrir un socle d’utilitaires standard, robustes et réutilisables pour tout le pipeline analytique :
  • Chargement fiable des CSV
  • Normalisation/nettoyage
  • Agrégations/calculeurs standards
  • Sauvegarde des résultats
"""

import pandas as pd
import os

# -------------------------------------------------------------------
# 📘 Chargement des fichiers CSV
# -------------------------------------------------------------------

def read_catalog(path: str):
    """
    Lecture du catalogue produits et harmonisation des colonnes.
    Retourne un DataFrame pandas standardisé avec :
    - product_id
    - product_name
    - category
    """
    import pandas as pd

    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]

    # Normalisation automatique des noms de colonnes
    rename_map = {
        "id": "product_id",
        "id_produit": "product_id",
        "produit_id": "product_id",
        "product": "product_name",
        "nom_produit": "product_name",
        "name": "product_name",
        "categorie": "category",
        "catégorie": "category",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Vérifie les colonnes essentielles
    for col in ["product_id", "product_name", "category"]:
        if col not in df.columns:
            df[col] = ""

    return df[["product_id", "product_name", "category"]]

# -------------------------------------------------------------------
# 🧹 Nettoyage et validation
# -------------------------------------------------------------------

def normalize_columns(df):
    """
    Uniformise tous les noms de colonnes d’un DataFrame :
    - en minuscules,
    - retire les espaces et remplace par '_'
    Permet une compatibilité robuste entre jeux de données hétérogènes.
    """
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def parse_sales_rows(df_or_path):
    """
    Nettoie et enrichit les données de ventes :
    - Accepte un DataFrame ou un chemin de fichier CSV
    - Gère les variations de noms de colonnes (ts/date, qty/quantity, etc.)
    - Supprime les lignes invalides
    - Calcule le revenu total et convertit les types
    """
    import pandas as pd
    import os

    # Lecture du fichier si on reçoit un chemin
    if isinstance(df_or_path, str):
        if not os.path.exists(df_or_path):
            raise FileNotFoundError(f"Fichier introuvable : {df_or_path}")
        df = pd.read_csv(df_or_path, on_bad_lines='skip')
    else:
        df = df_or_path.copy()

    # Harmonisation des noms de colonnes
    col_map = {
        "ts": "date",
        "timestamp": "date",
        "transaction_date": "date",
        "qty": "quantity",
        "quantite": "quantity",
        "prix_unitaire": "unit_price",
        "produit_id": "product_id",
        "pays": "country",
    }
    df = df.rename(columns={c.lower(): col_map.get(c.lower(), c.lower()) for c in df.columns})

    required_cols = ["product_id", "date", "country", "quantity", "unit_price"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes obligatoires manquantes : {missing}")

    # Nettoyage des valeurs manquantes
    df = df.dropna(subset=required_cols)

    # Conversion des types numériques
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # Filtrage des lignes valides
    df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]

    # Conversion du champ date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # Calcul du revenu total
    df["revenue"] = df["quantity"] * df["unit_price"]

    return df




# -------------------------------------------------------------------
# 📈 Agrégations communes
# -------------------------------------------------------------------

def aggregate_sales(df):
    """
    Calcule les agrégations de ventes par pays et par mois.
    Génère un DataFrame structuré pour le reporting :
      - country, year, month, total_sales, total_revenue
    """
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    agg = (
        df.groupby(["country", "year", "month"])
        .agg(total_sales=("quantity", "sum"), total_revenue=("revenue", "sum"))
        .reset_index()
    )
    return agg

# -------------------------------------------------------------------
# 💾 Sauvegarde des résultats
# -------------------------------------------------------------------

def save_dataframe(df, path, sep=","):
    """
    Enregistre un DataFrame sous forme de fichier CSV dans le répertoire de sortie.
    S’assure que le dossier parent existe bien. Affiche une notification de sauvegarde réussie.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, sep=sep)
    print(f"[INFO] Fichier sauvegardé : {path}")

def save_json(data, path):
    """
    Sauvegarde un dictionnaire Python (ou DataFrame) en fichier JSON lisible.
    S’assure que le dossier parent existe bien.
    """
    import json
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[INFO] JSON sauvegardé : {path}")

# -------------------------------------------------------------------
# 🧭 Journalisation simple (à compléter si besoin)
# -------------------------------------------------------------------
# (Exemples :
#    def log_message(msg): print(f"[LOG] {msg}")
#)
def log_step(title):
    """
    Affiche une étape claire et lisible dans la console.
    """
    print(f"\n=== {title} ===")