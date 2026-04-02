"""Find SKUs that exist in inventory but not in the merchant listings report."""
import os, sys
sys.path.insert(0, '.')
from src.monitor import (
    get_lwa_access_token, sp_api_headers, get_fba_inventory,
    _request_report, SP_API_BASE, MARKETPLACE_ID
)
import csv, io

token = get_lwa_access_token()
headers = sp_api_headers(token)

print("[1] Getting inventory...")
inventory = get_fba_inventory(token)
inv_skus = {item["sku"]: item["asin"] for item in inventory}
print(f"  Inventory: {len(inv_skus)} unique SKUs\n")

print("[2] Getting merchant listings report...")
content = _request_report(headers, "GET_MERCHANT_LISTINGS_ALL_DATA", MARKETPLACE_ID)
report_skus = set()
if content:
    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    for row in reader:
        sku = row.get("seller-sku", "").strip()
        if sku:
            report_skus.add(sku)
print(f"  Report: {len(report_skus)} SKUs\n")

# Find inventory SKUs missing from report
missing = []
for sku, asin in inv_skus.items():
    if sku not in report_skus:
        missing.append((asin, sku))

print(f"[3] SKUs in inventory but NOT in report: {len(missing)}\n")
for asin, sku in sorted(missing):
    # Try to find similar SKUs in report
    similar = [rs for rs in report_skus if asin in rs or sku[:10] in rs]
    sim_str = f" -> maybe: {similar[0]}" if similar else ""
    print(f"  {asin} | {sku}{sim_str}")
