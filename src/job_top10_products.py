# -*- coding: utf-8 -*-
"""
job_top10_products.py — Calcule les 10 produits les plus rentables.
Lecture directe du clean.csv et sortie formatée au format CSV classique.
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import os


class Top10Products(MRJob):
    """
    Étapes MapReduce :
    1️⃣ Mapper : lit chaque ligne et calcule le revenu (quantity × unit_price)
    2️⃣ Reducer intermédiaire : somme des revenus par produit
    3️⃣ Reducer final : extrait les 10 produits les plus rentables
    """

    def steps(self):
        """Définit explicitement les 3 étapes du job."""
        return [
            MRStep(mapper=self.mapper),
            MRStep(reducer=self.reducer_sum),
            MRStep(reducer=self.reducer_top10),
        ]

    def mapper(self, _, line):
        """Lit les lignes CSV et émet (product_id, revenu_total)."""
        if line.startswith("transaction_id"):
            self.header = line.strip().split(",")
            return

        try:
            row = next(csv.reader([line]))
            header = getattr(self, "header", [])
            row_dict = {header[i]: row[i] for i in range(len(header))}

            product_id = row_dict.get("product_id", "").strip()
            product_name = row_dict.get("product_name", "").strip()
            category = row_dict.get("category_y", "").strip()

            qty = float(row_dict.get("quantity", 0))
            price = float(row_dict.get("unit_price", 0))
            revenue = qty * price

            if product_id and revenue > 0:
                yield product_id, (product_name, category, revenue)

        except Exception:
            pass

    def reducer_sum(self, product_id, values):
        """Additionne les revenus par produit."""
        total_revenue = 0
        sample_name, sample_cat = "", ""
        for name, cat, rev in values:
            total_revenue += rev
            sample_name, sample_cat = name, cat
        yield None, (product_id, sample_name, sample_cat, round(total_revenue, 2))

    def reducer_top10(self, _, items):
        """Trie les 10 meilleurs produits."""
        top10 = sorted(items, key=lambda x: x[3], reverse=True)[:10]
        for pid, name, cat, total in top10:
            yield pid, [name, cat, total]


if __name__ == "__main__":
    job = Top10Products(args=["outputs/clean/clean.csv"])
    with job.make_runner() as runner:
        runner.run()
        results = list(job.parse_output(runner.cat_output()))

    os.makedirs("outputs/top10", exist_ok=True)
    out_path = "outputs/top10/top10_products.csv"

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "category", "total_revenue"])
        for pid, data in results:
            writer.writerow([pid] + list(data))

    print(f"[OK] Fichier local écrit : {out_path}")
    print("\n=== TOP 10 PRODUITS ===")
    for pid, data in results:
        print(f"{pid} | {data[0]} | {data[1]} | {data[2]:.2f}")
