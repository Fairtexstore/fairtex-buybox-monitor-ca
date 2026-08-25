"""Refresh CAD product costs from the USD source using the latest Bank of Canada FX rate.

Updates one artifact:
  1. product_costs.csv  — written from product_costs_usd.csv

Fairtex MSRP is NOT converted here. The "MSRP per Fairtex in CAD" column in
'Fairtex Price in USD.csv' is a locked CAD price supplied by Fairtex and is the
benchmark our listing prices are checked against. It must not be recomputed from
USD — doing so would overwrite the agreed CAD figures at whatever the month's
rate happens to be. The "Fairtex MSRP in USD" column is retained for reference
only and no longer drives anything.

Runs locally and from the monthly_fx_update GitHub Actions workflow on the 2nd of each month.
"""

import csv
import json
import os
import sys
import urllib.request

USD_CSV = "product_costs_usd.csv"
CAD_CSV = "product_costs.csv"
BOC_URL = "https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json?recent=1"


def fetch_usd_cad_rate():
    with urllib.request.urlopen(BOC_URL, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    obs = data["observations"][0]
    return float(obs["FXUSDCAD"]["v"]), obs["d"]


def convert_costs(here, rate):
    src = os.path.join(here, USD_CSV)
    dst = os.path.join(here, CAD_CSV)
    if not os.path.exists(src):
        print(f"WARN: {USD_CSV} not found, skipping COGS conversion")
        return 0

    rows = 0
    with open(src, "r", newline="", encoding="utf-8-sig") as fin, \
         open(dst, "w", newline="", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        writer = csv.writer(fout)
        writer.writerow(["ASIN", "FBA_Cost", "NARF_Cost"])
        for row in reader:
            asin = (row.get("ASIN") or "").strip()
            if not asin:
                continue
            fba_usd = float((row.get("FBA_Cost_USD") or "0").strip() or 0)
            narf_usd = float((row.get("NARF_Cost_USD") or "0").strip() or 0)
            writer.writerow([asin, round(fba_usd * rate, 4), round(narf_usd * rate, 4)])
            rows += 1
    return rows


def main():
    rate, fx_date = fetch_usd_cad_rate()
    print(f"USD->CAD rate (BoC daily avg, {fx_date}): {rate}")

    here = os.path.dirname(os.path.abspath(__file__))
    n_costs = convert_costs(here, rate)
    print(f"COGS: wrote {n_costs} rows to {CAD_CSV}")
    print("MSRP: skipped - 'MSRP per Fairtex in CAD' is a locked CAD price, not FX-derived")


if __name__ == "__main__":
    main()
