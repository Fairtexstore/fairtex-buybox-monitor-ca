"""Quick test: fetch fees for 10 FBA + 10 NARF ASINs and print results.
Run via GitHub Actions to verify fee accuracy without running the full monitor.
"""
import os
import time
import requests
from urllib.parse import quote
from src.monitor import (
    get_lwa_access_token, sp_api_headers, get_fba_inventory,
    get_fulfillment_types, check_buy_box,
    SP_API_BASE, MARKETPLACE_ID, AMAZON_USD_CAD_RATE
)

def test_fees():
    print("=== FEE TEST (10 FBA + 10 NARF) ===\n")

    print("[1] Getting access token...")
    token = get_lwa_access_token()
    headers = sp_api_headers(token)
    print("  OK\n")

    print("[2] Getting inventory (just first 2 pages for speed)...")
    # Minimal inventory fetch
    url = f"{SP_API_BASE}/fba/inventory/v1/summaries"
    params = {
        "details": "true",
        "granularityType": "Marketplace",
        "granularityId": MARKETPLACE_ID,
        "marketplaceIds": MARKETPLACE_ID,
    }
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    items_raw = resp.json().get("payload", {}).get("inventorySummaries", [])
    items = []
    for item in items_raw:
        sku = item.get("sellerSku", "")
        asin = item.get("asin", "")
        qty = (item.get("inventoryDetails") or {}).get("fulfillableQuantity") or 0
        if qty > 0 and asin and sku:
            items.append({"sku": sku, "asin": asin, "name": item.get("productName", "")[:50], "stock": qty})
    print(f"  Got {len(items)} SKUs from first page\n")

    print("[3] Classifying FBA vs NARF...")
    fba_asins = get_fulfillment_types(token)
    print()

    print("[4] Getting prices via buy box check (first batch only)...")
    batch_items = items[:20]
    buy_box_map = check_buy_box(token, batch_items)
    print()

    # Split into FBA and NARF
    fba_items = [i for i in batch_items if fba_asins and i["asin"] in fba_asins]
    narf_items = [i for i in batch_items if not fba_asins or i["asin"] not in fba_asins]

    test_items = fba_items[:10] + narf_items[:10]
    if len(fba_items) < 10:
        # Get more items for testing
        all_fba = [i for i in items if fba_asins and i["asin"] in fba_asins]
        all_narf = [i for i in items if not fba_asins or i["asin"] not in fba_asins]
        print(f"  Available: {len(all_fba)} FBA, {len(all_narf)} NARF in first page")

    print(f"\n[5] Testing fees for {len(test_items)} products...")
    print(f"    ({len([i for i in test_items if fba_asins and i['asin'] in fba_asins])} FBA, "
          f"{len([i for i in test_items if not fba_asins or i['asin'] not in fba_asins])} NARF)\n")

    print(f"{'ASIN':<14} {'SKU':<35} {'TYPE':<5} {'API_PRICE':>10} {'CAD_PRICE':>10} {'TOTAL_FEE':>10} {'METHOD':<8}")
    print("-" * 100)

    for item in test_items:
        asin = item["asin"]
        sku = item["sku"]
        is_narf = not fba_asins or asin not in fba_asins
        ft = "NARF" if is_narf else "FBA"

        info = buy_box_map.get(sku, {})
        msrp_str = info.get("our_msrp", "")
        try:
            api_price = float(msrp_str.replace("$", "").replace(",", "")) if msrp_str else 0
        except (ValueError, AttributeError):
            api_price = 0

        cad_price = api_price if is_narf else round(api_price * AMAZON_USD_CAD_RATE, 2)

        if cad_price <= 0:
            print(f"{asin:<14} {sku:<35} {ft:<5} {'N/A':>10} {'N/A':>10} {'N/A':>10} {'skip':<8}")
            continue

        body = {
            "FeesEstimateRequest": {
                "MarketplaceId": MARKETPLACE_ID,
                "IsAmazonFulfilled": True,
                "PriceToEstimateFees": {
                    "ListingPrice": {"CurrencyCode": "CAD", "Amount": cad_price},
                    "Shipping":     {"CurrencyCode": "CAD", "Amount": 0},
                },
                "Identifier": sku,
            }
        }

        total_fee = None
        method = ""

        # Try SKU endpoint first
        sku_encoded = quote(sku, safe="")
        sku_url = f"{SP_API_BASE}/products/fees/v0/listings/{sku_encoded}/feesEstimate"
        try:
            resp = requests.post(sku_url, headers=headers, json=body, timeout=30)
            print(f"    SKU endpoint: HTTP {resp.status_code}")
            if resp.status_code == 200:
                resp_json = resp.json()
                res = resp_json.get("payload", {}).get("FeesEstimateResult", {})
                if res.get("Status") == "Success":
                    total_fee = float(res["FeesEstimate"]["TotalFeesEstimate"]["Amount"])
                    method = "SKU"
                else:
                    err = res.get("Error", {})
                    print(f"    SKU FAIL: {res.get('Status')} - {err.get('Code','')} {err.get('Message','')[:150]}")
            else:
                print(f"    SKU HTTP error: {resp.text[:200]}")
        except Exception as e:
            print(f"    SKU exception: {e}")

        # Fallback to ASIN
        if total_fee is None:
            try:
                resp = requests.post(
                    f"{SP_API_BASE}/products/fees/v0/items/{asin}/feesEstimate",
                    headers=headers, json=body, timeout=30)
                if resp.status_code == 200:
                    res = resp.json().get("payload", {}).get("FeesEstimateResult", {})
                    if res.get("Status") == "Success":
                        total_fee = float(res["FeesEstimate"]["TotalFeesEstimate"]["Amount"])
                        method = "ASIN"
            except Exception:
                pass

        fee_str = f"${total_fee:.2f}" if total_fee else "FAILED"
        print(f"{asin:<14} {sku:<35} {ft:<5} ${api_price:>9.2f} ${cad_price:>9.2f} {fee_str:>10} {method:<8}")
        time.sleep(0.3)

    print("\n=== DONE ===")
    print("Compare TOTAL_FEE with Seller Central 'Estimated fees per unit sold' for each ASIN.")


if __name__ == "__main__":
    test_fees()
