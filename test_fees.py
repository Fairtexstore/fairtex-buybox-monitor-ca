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
    print("=== FEE TEST — Known SKUs ===\n")

    print("[1] Getting access token...")
    token = get_lwa_access_token()
    headers = sp_api_headers(token)
    print("  OK\n")

    # Hardcoded test cases with real seller SKUs from Seller Central
    # FBA products
    test_items = [
        {"sku": "CA-KPLC2 - StandardCurved", "asin": "B006CV6W6M", "type": "FBA", "sc_fee": 59.55},
        {"sku": "SP5-Black-Large",           "asin": "B00O1S1L64", "type": "FBA", "sc_fee": 46.38},
        {"sku": "SP5-Black-Medium",          "asin": "B00O1S1HUE", "type": "FBA", "sc_fee": None},
        {"sku": "HG10-Black/White-Medium",   "asin": "B00KR9CU8E", "type": "FBA", "sc_fee": None},
        {"sku": "KPLS2 - Black - STD",       "asin": "B00PM9XRZ4", "type": "FBA", "sc_fee": None},
        # NARF products
        {"sku": "FMV9 - Red/White",          "asin": "B006K40R1C", "type": "NARF", "sc_fee": 43.70},
        {"sku": "BGV14B - 14oz",             "asin": "B09QKKCP2V", "type": "NARF", "sc_fee": 49.14},
        {"sku": "HW2-Black-120",             "asin": "B0793KXHT9", "type": "NARF", "sc_fee": 14.32},
        {"sku": "BGV24-TheBeauty-16oz",      "asin": "B08MV8PCX8", "type": "NARF", "sc_fee": 43.23},
        {"sku": "SP5-Black-Small",           "asin": "B009QYOOUI", "type": "NARF", "sc_fee": None},
    ]

    # Get CAD prices from merchant listings report
    print("[2] Getting CAD prices from merchant listings report...")
    from src.monitor import _get_cad_prices_from_report, _request_report
    headers = sp_api_headers(token)
    cad_prices = _get_cad_prices_from_report(headers)
    print()

    print(f"{'ASIN':<14} {'SKU':<30} {'TYPE':<5} {'API$':>8} {'CAD$':>8} {'API_FEE':>8} {'SC_FEE':>8} {'DIFF':>6} {'METHOD':<6}")
    print("-" * 105)

    for item in test_items:
        asin = item["asin"]
        sku = item["sku"]
        is_narf = item["type"] == "NARF"
        ft = item["type"]
        sc_fee = item.get("sc_fee")

        cad_price = cad_prices.get(sku, 0)
        api_price = cad_price  # from report, already CAD

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

        fee_str = f"${total_fee:.2f}" if total_fee else "FAIL"
        sc_str = f"${sc_fee:.2f}" if sc_fee else "?"
        diff_str = ""
        if total_fee and sc_fee:
            diff = total_fee - sc_fee
            diff_str = f"{diff:+.2f}"
        print(f"{asin:<14} {sku:<30} {ft:<5} {api_price:>8.2f} {cad_price:>8.2f} {fee_str:>8} {sc_str:>8} {diff_str:>6} {method:<6}")
        time.sleep(0.3)

    print("\n=== DONE ===")
    print("DIFF = API fee - Seller Central fee (negative = API underestimates)")


if __name__ == "__main__":
    test_fees()
