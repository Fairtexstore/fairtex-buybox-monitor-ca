"""Refresh CAD values from USD source files using the latest Bank of Canada FX rate.

Updates two artifacts at the same rate:
  1. product_costs.csv         — written from product_costs_usd.csv
  2. Fairtex Price in USD.csv  — recomputes the "MSRP per Fairtex in CAD" column in place

Runs locally and from the monthly_fx_update GitHub Actions workflow on the 2nd of each month.
"""

import csv
import json
import math
import os
import sys
import urllib.request

USD_CSV = "product_costs_usd.csv"
CAD_CSV = "product_costs.csv"
MSRP_CSV = "Fairtex Price in USD.csv"
MSRP_USD_COL = "Fairtex MSRP in USD"
MSRP_CAD_COL = "MSRP per Fairtex in CAD"
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


def convert_msrp(here, rate):
    path = os.path.join(here, MSRP_CSV)
    if not os.path.exists(path):
        print(f"WARN: '{MSRP_CSV}' not found, skipping MSRP conversion")
        return 0

    with open(path, "r", newline="", encoding="utf-8-sig") as fin:
        reader = csv.DictReader(fin)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    if MSRP_CAD_COL not in fieldnames:
        fieldnames.append(MSRP_CAD_COL)

    count = 0
    for row in rows:
        usd_str = (row.get(MSRP_USD_COL) or "").strip()
        if not usd_str:
            row[MSRP_CAD_COL] = ""
            continue
        try:
            usd = float(usd_str)
        except ValueError:
            row[MSRP_CAD_COL] = ""
            continue
        cad = usd * rate
        # Round UP to the next X.99 (e.g. 85.01 -> 85.99, 85.99 -> 85.99, 86.00 -> 86.99).
        # 0.995 epsilon prevents float noise on values already at X.99 from
        # jumping to (X+1).99.
        cad_rounded = math.ceil(cad - 0.995) + 0.99
        row[MSRP_CAD_COL] = f"{cad_rounded:.2f}"
        count += 1

    with open(path, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return count


def main():
    rate, fx_date = fetch_usd_cad_rate()
    print(f"USD->CAD rate (BoC daily avg, {fx_date}): {rate}")

    here = os.path.dirname(os.path.abspath(__file__))
    n_costs = convert_costs(here, rate)
    print(f"COGS: wrote {n_costs} rows to {CAD_CSV}")
    n_msrp = convert_msrp(here, rate)
    print(f"MSRP: updated {n_msrp} rows in '{MSRP_CSV}'")


if __name__ == "__main__":
    main()
