# -*- coding: utf-8 -*-
"""
job_kpi_sales_by_country_month.py — Calcule le total des ventes par pays et par mois (KPI).
Entrée : outputs/clean/clean.csv
Sortie : outputs/metrics/sales_by_country_month.csv
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import os


class KPIByCountryMonth(MRJob):
    """Calcule les ventes agrégées par couple (country, yyyy_mm)."""

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer_sum)]

    def mapper(self, _, line):
        """Lit les lignes du fichier nettoyé et émet (country, yyyy_mm) → montant."""
        if line.startswith("transaction_id"):
            self.header = line.strip().split(",")
            return

        try:
            row = next(csv.reader([line]))
            header = getattr(self, "header", [])
            row_dict = {header[i]: row[i] for i in range(len(header))}

            country = row_dict.get("country", "").strip()
            month = row_dict.get("yyyy_mm", "").strip()
            revenue = float(row_dict.get("net_amount", 0))

            if country and month and revenue > 0:
                yield (country, month), revenue

        except Exception:
            pass

    def reducer_sum(self, key, values):
        """Somme le revenu total pour chaque pays et mois."""
        yield key, round(sum(values), 2)


if __name__ == "__main__":
    job = KPIByCountryMonth(args=["outputs/clean/clean.csv"])
    with job.make_runner() as runner:
        runner.run()
        results = list(job.parse_output(runner.cat_output()))

    os.makedirs("outputs/metrics", exist_ok=True)
    out_path = "outputs/metrics/sales_by_country_month.csv"

    # Sauvegarde CSV classique
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["country", "month", "total_sales"])
        for (country, month), total in results:
            writer.writerow([country, month, total])

    # Affichage console lisible
    print(f"[OK] Fichier local écrit : {out_path}")
    print("\n=== KPI VENTES PAR PAYS ET MOIS ===")
    print(f"{'Pays':<10} {'Mois':<10} {'Total ventes ($)':>15}")
    print("-" * 40)
    for (country, month), total in results[:15]:
        print(f"{country:<10} {month:<10} {total:>15.2f}")
