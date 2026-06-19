"""Convert product_costs_usd.csv (USD) → product_costs.csv (CAD).

Fetches the latest daily USD/CAD rate from the Bank of Canada Valet API
and writes the CAD-denominated file that src/monitor.py reads.

Runs locally and from the monthly_fx_update GitHub Actions workflow on the
2nd of each month.
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


def main():
    rate, fx_date = fetch_usd_cad_rate()
    print(f"USD->CAD rate (BoC daily avg, {fx_date}): {rate}")

    here = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(here, USD_CSV)
    dst = os.path.join(here, CAD_CSV)

    if not os.path.exists(src):
        print(f"ERROR: {src} not found", file=sys.stderr)
        sys.exit(1)

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

    print(f"Wrote {rows} rows to {CAD_CSV} at rate {rate} ({fx_date})")


if __name__ == "__main__":
    main()
