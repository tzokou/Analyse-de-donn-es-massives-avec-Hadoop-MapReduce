# -*- coding: utf-8 -*-
"""
job_return_rate.py — Calcule le taux global de retour (is_return == 1).
Entrée : outputs/clean/clean.csv
Sortie : outputs/metrics/return_rate.jsonl
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import os


class ReturnRate(MRJob):
    """Calcule le taux de retour global."""

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer_rate)]

    def mapper(self, _, line):
        """Émet (clé unique, total_retours, total_transactions)."""
        if line.startswith("transaction_id"):
            return
        try:
            parts = line.strip().split(",")
            is_return = parts[15].strip() if len(parts) > 15 else "0"
            yield "global", (1, 1 if is_return == "1" else 0)
        except Exception:
            pass

    def reducer_rate(self, _, values):
        """Calcule le taux de retour global."""
        total, returned = 0, 0
        for t, r in values:
            total += t
            returned += r
        rate = (returned / total * 100) if total > 0 else 0
        yield None, {"total": total, "returned": returned, "return_rate(%)": round(rate, 2)}


if __name__ == "__main__":
    job = ReturnRate(args=["outputs/clean/clean.csv"])
    with job.make_runner() as runner:
        runner.run()
        results = list(job.parse_output(runner.cat_output()))

    os.makedirs("outputs/metrics", exist_ok=True)
    out_path = "outputs/metrics/return_rate.jsonl"

    # Sauvegarde JSONL
    with open(out_path, "w", encoding="utf-8") as f:
        for _, data in results:
            json.dump(data, f)
            f.write("\n")

    # Affichage console lisible
    print(f"[OK] Fichier local écrit : {out_path}")
    print("\n=== TAUX DE RETOUR GLOBAL ===")
    for _, data in results:
        print(f"Total transactions : {data['total']}")
        print(f"Transactions retournées : {data['returned']}")
        print(f"Taux de retour global : {data['return_rate(%)']:.2f} %")
