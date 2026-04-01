import csv
import gzip
import io
import json
import os
import time
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

LWA_TOKEN_URL  = "https://api.amazon.com/auth/o2/token"
MARKETPLACE_ID = os.environ.get("AMAZON_MARKETPLACE_ID", "A2EUQ1WTGCTBG2")
US_MARKETPLACE = "ATVPDKIKX0DER"
SP_API_BASE    = "https://sellingpartnerapi-na.amazon.com"
SLACK_CHANNEL  = "C0AMDJ91151"
MY_SELLER_ID   = "A1LC1HJLF7IAWT"
NARF_IMPORT_FEE_RATE = 0.14  # 14% import/customs fee for NARF cross-border orders
# Amazon's internal USD→CAD rate for FBA cross-border sellers.
# SP-API returns USD for FBA products. This rate converts to the actual CAD price
# shown on Seller Central (e.g. $227.47 USD × 1.1447 = $260.39 CAD).
# NARF products already return CAD from the API — no conversion needed.
AMAZON_USD_CAD_RATE = 1.1447



def get_lwa_access_token():
    resp = requests.post(LWA_TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "refresh_token": os.environ["AMAZON_REFRESH_TOKEN"],
        "client_id":     os.environ["AMAZON_CLIENT_ID"],
        "client_secret": os.environ["AMAZON_CLIENT_SECRET"],
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def sp_api_headers(access_token):
    return {
        "x-amz-access-token": access_token,
        "Content-Type":       "application/json",
        "Accept":             "application/json",
    }


_seller_names = {}

def _load_seller_names():
    global _seller_names
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "seller_names.json")
    try:
        with open(path, "r") as f:
            _seller_names = json.load(f)
    except Exception:
        _seller_names = {}

def get_seller_name(seller_id):
    """Look up seller display name from seller_names.json. Falls back to seller ID."""
    if not _seller_names:
        _load_seller_names()
    return _seller_names.get(seller_id, seller_id)


def load_product_costs():
    """Load product cost data from product_costs.csv. Returns dict keyed by ASIN.
    Each entry has fba_cost and narf_cost; caller selects the right one based on fulfillment type.
    """
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "product_costs.csv")
    costs = {}
    try:
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                asin = row.get("ASIN", "").strip()
                if not asin:
                    continue
                def _parse(val):
                    v = (val or "").strip().replace("$", "").replace(",", "")
                    try:
                        return float(v) if v else None
                    except ValueError:
                        return None
                costs[asin] = {
                    "fba_cost":  _parse(row.get("FBA_Cost")),
                    "narf_cost": _parse(row.get("NARF_Cost")),
                }
        print(f"  Loaded cost data for {len(costs)} ASINs")
    except FileNotFoundError:
        print("  product_costs.csv not found - skipping cost data")
    except Exception as e:
        print(f"  Error loading product_costs.csv: {e}")
    return costs


def compute_recommendation(winner_price_str, lowest_msrp):
    """Compute pricing recommendation based on winner price vs minimum MSRP."""
    if not winner_price_str or lowest_msrp is None:
        return ""
    try:
        winner_price = float(winner_price_str.replace("$", "").replace(",", ""))
    except ValueError:
        return ""
    if winner_price > lowest_msrp:
        return f"Yes, reduce to ${lowest_msrp:.2f}"
    else:
        return "No, winner below minimum"


def _request_report(headers, report_type, marketplace_id):
    """Request an SP-API report and wait for completion. Returns content string or None."""
    resp = requests.post(
        f"{SP_API_BASE}/reports/2021-06-30/reports",
        headers=headers,
        json={"reportType": report_type, "marketplaceIds": [marketplace_id]},
        timeout=30,
    )
    if resp.status_code == 429:
        time.sleep(30)
        resp = requests.post(
            f"{SP_API_BASE}/reports/2021-06-30/reports",
            headers=headers,
            json={"reportType": report_type, "marketplaceIds": [marketplace_id]},
            timeout=30,
        )
    if resp.status_code not in (200, 202):
        print(f"  Failed to create report: {resp.status_code}: {resp.text[:300]}")
        return None

    report_id = resp.json().get("reportId")
    if not report_id:
        return None
    print(f"  Report ID: {report_id}")

    # Poll until done
    doc_id = None
    for attempt in range(30):
        time.sleep(10)
        resp = requests.get(
            f"{SP_API_BASE}/reports/2021-06-30/reports/{report_id}",
            headers=headers, timeout=30,
        )
        if resp.status_code != 200:
            continue
        status = resp.json().get("processingStatus", "")
        print(f"  Report status: {status} (attempt {attempt + 1})")
        if status == "DONE":
            doc_id = resp.json().get("reportDocumentId")
            break
        if status in ("CANCELLED", "FATAL"):
            return None
    if not doc_id:
        return None

    # Get document URL and download
    resp = requests.get(
        f"{SP_API_BASE}/reports/2021-06-30/documents/{doc_id}",
        headers=headers, timeout=30,
    )
    if resp.status_code != 200:
        return None

    doc_info = resp.json()
    doc_url = doc_info.get("url")
    compression = doc_info.get("compressionAlgorithm")
    if not doc_url:
        return None

    resp = requests.get(doc_url, timeout=60)
    if resp.status_code != 200:
        return None

    raw = resp.content
    if compression == "GZIP":
        raw = gzip.decompress(raw)

    print(f"  Downloaded {len(raw)} bytes")
    return raw.decode("utf-8", errors="replace")


def get_fulfillment_types(access_token):
    """Classify Canada inventory as FBA or NARF using the planning report's
    age bucket data — the same 'FBA inventory age by days' visible on each
    product's Seller Central page.

    Uses GET_FBA_INVENTORY_PLANNING_DATA for Canada marketplace.
    Matches by ASIN (not SKU) because SKU formatting differs between the
    report and the inventory API.

    - Any age bucket > 0 → FBA (inventory in Canadian FCs)
    - All age buckets = 0 or not in report → NARF

    Returns a set of FBA ASINs, or None if report failed.
    """
    headers = sp_api_headers(access_token)

    print("  Fetching FBA Inventory Planning Data for Canada...")
    content = _request_report(headers, "GET_FBA_INVENTORY_PLANNING_DATA", MARKETPLACE_ID)
    if content is None:
        print("  WARNING: Report failed — cannot classify FBA vs NARF")
        return None

    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    fieldnames = reader.fieldnames or []

    age_columns = [
        "inv-age-0-to-90-days", "inv-age-91-to-180-days",
        "inv-age-181-to-270-days", "inv-age-271-to-365-days",
        "inv-age-365-plus-days",
    ]

    # Known FBA ASINs to verify report accuracy
    verify_asins = {"B00O1S1HUE", "B00O1S1OFW", "B00PM9XRZ4", "B07B2Z8P7S"}

    fba_asins = set()
    all_report_asins = set()
    total_rows = 0

    for row in reader:
        asin = row.get("asin", "").strip()
        sku = row.get("sku", row.get("seller-sku", "")).strip()
        if not asin:
            continue
        total_rows += 1
        all_report_asins.add(asin)

        age_vals = {col: row.get(col, "0").strip() for col in age_columns}
        has_aged = any(int(v or "0") > 0 for v in age_vals.values())

        if asin in verify_asins:
            print(f"  VERIFY {asin} (SKU={sku}): age={age_vals}, FBA={has_aged}")

        if has_aged:
            fba_asins.add(asin)

    # Check if known FBA ASINs are even in the report
    for va in verify_asins:
        if va not in all_report_asins:
            print(f"  MISSING: {va} is NOT in the planning report at all")

    print(f"  Report had {total_rows} rows, {len(all_report_asins)} unique ASINs")
    print(f"  {len(fba_asins)} ASINs with age > 0 (FBA)")
    return fba_asins


def get_fba_inventory(access_token):
    headers    = sp_api_headers(access_token)
    url        = f"{SP_API_BASE}/fba/inventory/v1/summaries"
    all_items  = []
    seen_skus  = set()
    next_token = None
    page       = 0
    start_date = (datetime.now(timezone.utc) - timedelta(days=540)).strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        page  += 1
        params = {
            "details":         "true",
            "granularityType": "Marketplace",
            "granularityId":   MARKETPLACE_ID,
            "marketplaceIds":  MARKETPLACE_ID,
            "startDateTime":   start_date,
        }
        if next_token:
            params["nextToken"] = next_token

        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            print(f"  Page {page} rate limited, waiting 5s...")
            time.sleep(5)
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            # Next token may have expired — restart pagination without token
            if next_token and "invalid" in resp.text.lower():
                print(f"  Page {page} next token expired, restarting pagination...")
                next_token = None
                # Use a later startDateTime to skip already-fetched items
                params.pop("nextToken", None)
                resp = requests.get(url, headers=headers, params=params, timeout=30)
            else:
                print(f"  Page {page} error {resp.status_code}, retrying in 5s...")
                time.sleep(5)
                resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  Page {page} failed: {resp.status_code}: {resp.text[:200]}")
            break

        data  = resp.json()
        items = data.get("payload", {}).get("inventorySummaries", [])

        for item in items:
            sku = item.get("sellerSku", "")
            if sku and sku not in seen_skus:
                seen_skus.add(sku)
                all_items.append(item)

        print(f"  Page {page}: {len(items)} records (unique so far: {len(all_items)})")

        next_token = (data.get("pagination") or {}).get("nextToken")
        if not next_token:
            next_token = (data.get("payload") or {}).get("nextToken")
        if not next_token:
            break
        time.sleep(1)

    print(f"  Total unique records: {len(all_items)}")

    result = []
    for item in all_items:
        sku   = item.get("sellerSku", "")
        asin  = item.get("asin", "")
        fnsku = item.get("fnsku", "")
        name  = item.get("productName", asin)[:70]
        qty   = (item.get("inventoryDetails") or {}).get("fulfillableQuantity") or 0

        sku_lower = sku.lower()
        if (sku_lower.startswith("amzn.gr") or
            sku_lower.startswith("dnu") or
            sku_lower.endswith("_ln") or
            fnsku.startswith("X")):
            continue
        if qty > 0 and asin and sku:
            result.append({"sku": sku, "asin": asin, "name": name, "stock": qty})

    print(f"  SKUs with fulfillable stock > 0: {len(result)}")
    return result


def check_buy_box(access_token, items):
    headers    = sp_api_headers(access_token)
    result     = {}
    asin_msrp  = {}
    total      = len(items)
    batch_size = 20

    print(f"  Checking {total} SKUs via batch listing offers...")

    for i in range(0, total, batch_size):
        batch = items[i:i + batch_size]

        batch_requests = []
        for item in batch:
            batch_requests.append({
                "method": "GET",
                "uri": f"/products/pricing/v0/listings/{requests.utils.quote(item['sku'], safe='')}/offers",
                "MarketplaceId": MARKETPLACE_ID,
                "ItemCondition": "New",
                "CustomerType":  "Consumer",
            })

        resp = requests.post(
            f"{SP_API_BASE}/batches/products/pricing/v0/listingOffers",
            headers=headers,
            json={"requests": batch_requests},
            timeout=60,
        )

        if resp.status_code == 429:
            print(f"  Rate limited at batch {i // batch_size + 1}, waiting 30s...")
            time.sleep(30)
            resp = requests.post(
                f"{SP_API_BASE}/batches/products/pricing/v0/listingOffers",
                headers=headers,
                json={"requests": batch_requests},
                timeout=60,
            )

        if resp.status_code != 200:
            print(f"  Batch error {resp.status_code}: {resp.text[:200]}")
            for item in batch:
                result[item["sku"]] = {"has_buy_box": True}
            time.sleep(3)
            continue

        responses = resp.json().get("responses", [])

        for j, item in enumerate(batch):
            sku = item["sku"]
            if j >= len(responses):
                result[sku] = {"has_buy_box": True}
                continue

            res   = responses[j]
            body  = res.get("body", {})
            error = body.get("errors", [None])[0] if body.get("errors") else None

            if error:
                code = error.get("code", "")
                msg  = error.get("message", "")
                if code == "InvalidInput" and "is an invalid SKU for marketplace" in msg:
                    result[sku] = {"has_buy_box": True}
                else:
                    result[sku] = {"has_buy_box": True}
                continue

            offers = body.get("payload", {}).get("Offers", [])
            our_offer = next((o for o in offers if o.get("SellerId") == MY_SELLER_ID), None)

            # Extract our listing price and landed price
            price_source = our_offer
            winner = next((o for o in offers if o.get("IsBuyBoxWinner")), None)
            if not price_source and winner and winner.get("SellerId") == MY_SELLER_ID:
                price_source = winner
            our_msrp = ""
            our_landed = ""
            if price_source:
                lp = price_source.get("ListingPrice", {}).get("Amount")
                sp = price_source.get("Shipping", {}).get("Amount")
                if lp is not None:
                    our_msrp = f"${float(lp):.2f}"
                    asin_msrp[item["asin"]] = our_msrp
                    # Landed price = listing price + shipping/import fees
                    landed = float(lp) + (float(sp) if sp else 0)
                    our_landed = f"${landed:.2f}"

            we_have_it = (
                (our_offer and our_offer.get("IsBuyBoxWinner") is True) or
                (winner and winner.get("SellerId") == MY_SELLER_ID)
            )

            if we_have_it:
                result[sku] = {"has_buy_box": True, "our_msrp": our_msrp, "our_landed": our_landed}
            else:
                winner_id     = winner.get("SellerId", "Unknown") if winner else None
                winner_seller = get_seller_name(winner_id) if winner_id else "No winner"
                winner_url    = f"https://www.amazon.ca/sp?seller={winner_id}" if winner_id else ""
                winner_price  = ""
                if winner:
                    lp = winner.get("ListingPrice", {}).get("Amount")
                    sp = winner.get("Shipping", {}).get("Amount")
                    if lp is not None:
                        landed = float(lp) + (float(sp) if sp else 0)
                        winner_price = f"${landed:.2f}"
                result[sku] = {
                    "has_buy_box":    False,
                    "our_msrp":       our_msrp,
                    "our_landed":     our_landed,
                    "winner_seller":  winner_seller,
                    "winner_url":     winner_url,
                    "winner_price":   winner_price,
                }

        if (i + batch_size) % 100 < batch_size:
            print(f"  Progress: {min(i + batch_size, total)}/{total}")

        time.sleep(1)

    # Backfill missing MSRPs using ASIN mapping
    backfilled = 0
    for item in items:
        sku = item["sku"]
        if sku in result and not result[sku].get("our_msrp") and item["asin"] in asin_msrp:
            result[sku]["our_msrp"] = asin_msrp[item["asin"]]
            backfilled += 1
    if backfilled:
        print(f"  Backfilled MSRP for {backfilled} SKUs via ASIN mapping")

    # Fetch remaining missing MSRPs by ASIN via getPricing API
    missing_asins = set()
    for item in items:
        if item["sku"] in result and not result[item["sku"]].get("our_msrp"):
            missing_asins.add(item["asin"])
    missing_asins = list(missing_asins)

    if missing_asins:
        print(f"  Fetching {len(missing_asins)} remaining MSRPs by ASIN...")
        fetched = 0
        for asin in missing_asins:
            url = f"{SP_API_BASE}/products/pricing/v0/price?MarketplaceId={MARKETPLACE_ID}&ItemType=Asin&Asins={asin}"
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 429:
                    time.sleep(30)
                    resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    for pd in resp.json().get("payload", []):
                        if pd.get("status") != "Success":
                            continue
                        ol = pd.get("Product", {}).get("Offers", [])
                        if ol:
                            lp = ol[0].get("BuyingPrice", {}).get("ListingPrice", {}).get("Amount")
                            if lp is not None:
                                asin_msrp[asin] = f"${float(lp):.2f}"
                                fetched += 1
            except Exception:
                pass
            time.sleep(0.3)
        print(f"  Fetched {fetched}/{len(missing_asins)} via getPricing")

        # Apply to all SKUs with those ASINs
        for item in items:
            sku = item["sku"]
            if sku in result and not result[sku].get("our_msrp") and item["asin"] in asin_msrp:
                result[sku]["our_msrp"] = asin_msrp[item["asin"]]

    owned     = sum(1 for v in result.values() if v.get("has_buy_box"))
    not_owned = len(result) - owned
    print(f"  Buy box owned: {owned} | NOT owned: {not_owned}")
    return result


def post_slack(headers, text):
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=headers,
        json={"channel": SLACK_CHANNEL, "text": text},
    )
    if not resp.json().get("ok"):
        raise RuntimeError(f"Slack error: {resp.json().get('error')}")
    time.sleep(0.3)


def send_slack_alert(flagged, total_checked, dashboard_url=""):
    headers = {
        "Authorization": f"Bearer {os.environ['SLACK_BOT_TOKEN']}",
        "Content-Type":  "application/json",
    }
    now_cst = datetime.now(ZoneInfo("America/Chicago")).strftime("%b %d, %Y %I:%M %p CST")

    if not flagged:
        post_slack(headers,
            f":white_check_mark: *Amazon CA Buy Box Check - {now_cst}*\n"
            f"Checked *{total_checked} SKUs*. All currently have the featured offer. Nothing to action."
        )
        return

    unique_asins = len(set(p["asin"] for p in flagged))
    post_slack(headers,
        f":warning: *Amazon CA Buy Box Alert - {now_cst}*\n"
        f"Checked *{total_checked} SKUs* — *{len(flagged)} SKU(s)* ({unique_asins} ASIN(s)) do NOT have the buy box.\n\n"
        f"<@U04DSUU9KGT> Please check the Canada dashboard for details and take action.\n"
        f"{dashboard_url}"
    )




def _get_fees_per_asin(headers, items, buy_box_map, fba_asins=None):
    """For each ASIN:
    1. Get the actual CAD price from getCompetitivePricing
    2. Send that CAD price to the fees endpoint
       - FBA products: standard FBA fulfillment fee
       - NARF products: Remote Fulfillment with FBA fee (FBA_EFN)
    3. Get back the correct total fees in CAD (matching Seller Central)
    """
    url_base = f"{SP_API_BASE}/products/fees/v0/items"
    fee_map = {}

    # Get unique ASINs
    unique_asins = []
    seen = set()
    for item in items:
        asin = item["asin"]
        if asin not in seen:
            seen.add(asin)
            unique_asins.append(asin)

    # Build list of (sku, asin, price) for fee lookups.
    # Send SP-API price as-is — no USD→CAD conversion.
    # The SKU endpoint handles currency internally and gives exact fees.
    sku_list = []
    seen_skus = set()
    for item in items:
        sku = item["sku"]
        asin = item["asin"]
        if sku not in seen_skus:
            seen_skus.add(sku)
            info = buy_box_map.get(sku, {})
            msrp_str = info.get("our_msrp", "")
            try:
                api_price = float(msrp_str.replace("$", "").replace(",", "")) if msrp_str else 50.0
            except (ValueError, AttributeError):
                api_price = 50.0
            is_narf = fba_asins is not None and asin not in fba_asins
            sku_list.append((sku, asin, api_price, is_narf))

    total = len(sku_list)
    print(f"  Fetching total fees for {total} SKUs via listings endpoint...")

    from urllib.parse import quote
    for idx, (sku, asin, api_price, is_narf) in enumerate(sku_list):
        cad_price = api_price  # sent as-is, SKU endpoint handles currency
        if cad_price <= 0:
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
        sku_encoded = quote(sku, safe="")
        url = f"{SP_API_BASE}/products/fees/v0/listings/{sku_encoded}/feesEstimate"

        try:
            resp = requests.post(url, headers=headers, json=body, timeout=30)
            if resp.status_code == 429:
                time.sleep(30)
                resp = requests.post(url, headers=headers, json=body, timeout=30)

            success = False
            if resp.status_code == 200:
                payload = resp.json().get("payload", {})
                res = payload.get("FeesEstimateResult", {})
                if res.get("Status") == "Success":
                    total_fees_obj = res.get("FeesEstimate", {}).get("TotalFeesEstimate", {})
                    total_fee = float(total_fees_obj.get("Amount", 0) or 0)
                    currency = total_fees_obj.get("CurrencyCode", "?")
                    fee_map[asin] = {"total_fee": round(total_fee, 2), "cad_price": cad_price}
                    ft_label = "NARF" if is_narf else "FBA"
                    if idx < 5:
                        print(f"  Sample: {sku} ({ft_label}) price=${cad_price} fee={currency}${total_fee}")
                    success = True

            # Fallback to ASIN endpoint if SKU endpoint failed
            if not success and asin not in fee_map:
                try:
                    resp2 = requests.post(
                        f"{SP_API_BASE}/products/fees/v0/items/{asin}/feesEstimate",
                        headers=headers, json=body, timeout=30)
                    if resp2.status_code == 200:
                        res2 = resp2.json().get("payload", {}).get("FeesEstimateResult", {})
                        if res2.get("Status") == "Success":
                            tf2 = res2.get("FeesEstimate", {}).get("TotalFeesEstimate", {})
                            total_fee = float(tf2.get("Amount", 0) or 0)
                            fee_map[asin] = {"total_fee": round(total_fee, 2), "cad_price": cad_price}
                            if idx < 5:
                                print(f"  Fallback OK: {asin} fee=${total_fee}")
                except Exception:
                    pass
        except Exception as e:
            if idx < 3:
                print(f"  Exception for {sku}: {e}")

        if (idx + 1) % 100 == 0:
            print(f"  Fees progress: {idx + 1}/{total}")
        time.sleep(0.5)

    print(f"  Fees retrieved for {len(fee_map)} ASINs out of {total} SKUs")
    return fee_map


def get_fee_estimates(access_token, items=None, buy_box_map=None, fba_asins=None):
    """Fetch per-ASIN total fees in CAD.

    Uses SKU-based endpoint with SP-API prices as-is (no conversion).
    The SKU endpoint handles currency internally and gives exact fees.
    Falls back to ASIN endpoint for SKUs that fail.

    Returns fee_map: asin -> {total_fee, cad_price}
    """
    headers = sp_api_headers(access_token)

    if items and buy_box_map:
        fee_map = _get_fees_per_asin(headers, items, buy_box_map, fba_asins)
    else:
        print("  WARNING: No items/buy_box_map — fees will be 0")
        fee_map = {}

    return fee_map


def main():
    print("=== Amazon CA Buy Box Monitor ===")
    print(f"Time (UTC): {datetime.now(timezone.utc).isoformat()}")

    print("\n[1/8] Fetching LWA access token...")
    access_token = get_lwa_access_token()
    print("  OK")

    print("\n[2/8] Fetching FBA inventory...")
    inventory = get_fba_inventory(access_token)
    print(f"  {len(inventory)} SKUs in stock.")

    if not inventory:
        send_slack_alert([], 0)
        return

    print("\n[3/8] Checking buy box and prices per SKU...")
    buy_box_map = check_buy_box(access_token, inventory)

    print("\n[4/8] Classifying FBA vs NARF...")
    fba_asins = get_fulfillment_types(access_token)
    # fba_asins = set of ASINs with Canadian FC inventory (FBA)
    # ASINs NOT in this set = NARF
    if fba_asins is not None:
        fba = sum(1 for item in inventory if item["asin"] in fba_asins)
        narf = len(inventory) - fba
        print(f"  Canada inventory: {fba} FBA, {narf} NARF (out of {len(inventory)})")

    print("\n[5/8] Loading product cost data...")
    product_costs = load_product_costs()

    print("\n[6/8] Fetching fee estimates (referral + fulfillment) per ASIN...")
    fee_estimates = get_fee_estimates(access_token, inventory, buy_box_map, fba_asins)

    def _build_total_cost(asin, ft, cost_data):
        """Total Cost = Product Cost (CAD) + Amazon Total Fees (CAD).
        Product cost comes from CSV (FBA or NARF column based on fulfillment type).
        Total fee comes from Amazon's per-ASIN fee estimate (all fee components).
        """
        if not cost_data:
            return None
        product_cost = cost_data["fba_cost"] if ft == "FBA" else cost_data["narf_cost"]
        if product_cost is None:
            return None
        fees = fee_estimates.get(asin, {})
        total_fee = fees.get("total_fee", 0) or 0
        return round(product_cost + total_fee, 2)

    def _get_cad_msrp(asin, fallback_info):
        """Get the CAD MSRP. For FBA products, the fee estimates store
        the converted CAD price. For NARF, SP-API price is already CAD."""
        fee_data = fee_estimates.get(asin, {})
        cad_price = fee_data.get("cad_price")
        if cad_price:
            return f"${cad_price:.2f}"
        return fallback_info.get("our_msrp", "")


    print("\n[7/8] Flagging and alerting...")
    flagged = []
    for item in inventory:
        info = buy_box_map.get(item["sku"], {})
        ft = ("FBA" if item["asin"] in fba_asins else "NARF") if fba_asins is not None else "Unknown"
        if not info.get("has_buy_box"):
            item["winner_seller"] = info.get("winner_seller", "Unknown")
            item["winner_url"]    = info.get("winner_url", "")
            item["winner_price"]  = info.get("winner_price", "")
            total_cost = _build_total_cost(item["asin"], ft, product_costs.get(item["asin"]))
            lowest_msrp = round(total_cost / 0.85, 2) if total_cost is not None else None
            item["total_cost"]     = total_cost
            item["lowest_msrp"]    = lowest_msrp
            item["recommendation"] = compute_recommendation(item["winner_price"], lowest_msrp)
            flagged.append(item)
    print(f"  Flagged: {len(flagged)}")
    for p in flagged:
        rec_str = f" | Rec: {p['recommendation']}" if p.get("recommendation") else ""
        print(f"    - {p['sku']} | {p['asin']} | stock: {p['stock']} | winner: {p['winner_seller']} @ {p['winner_price']}{rec_str}")

    send_slack_alert(flagged, len(inventory), "https://fairtex-buybox-monitor-ca.vercel.app/")

    # Save results to JSON for the Vercel dashboard
    print("\n[8/8] Saving dashboard data...")
    all_products = []
    for item in inventory:
        info = buy_box_map.get(item["sku"], {})
        cost_data = product_costs.get(item["asin"])
        ft = ("FBA" if item["asin"] in fba_asins else "NARF") if fba_asins is not None else "Unknown"
        # Use CAD MSRP from fee report (authoritative) instead of USD from SP-API
        our_msrp = _get_cad_msrp(item["asin"], info)
        our_landed = our_msrp  # default
        # For FBA: landed price = MSRP (no extra fees)
        # For NARF: landed price = MSRP + 14% import fees
        if ft == "NARF" and our_msrp:
            try:
                msrp_val = float(our_msrp.replace("$", "").replace(",", ""))
                our_landed = f"${msrp_val * (1 + NARF_IMPORT_FEE_RATE):.2f}"
            except ValueError:
                pass
        # Total cost = product cost + referral fee + fulfillment fee (FBA or NARF)
        total_cost  = _build_total_cost(item["asin"], ft, cost_data)
        lowest_msrp = round(total_cost / 0.85, 2) if total_cost is not None else None
        product = {
            "sku":              item["sku"],
            "asin":             item["asin"],
            "name":             item["name"],
            "stock":            item["stock"],
            "our_msrp":         our_msrp,
            "our_landed":       our_landed,
            "has_buy_box":      info.get("has_buy_box", True),
            "total_cost":       total_cost,
            "lowest_msrp":      lowest_msrp,
            "fulfillment_type": ft,
        }
        if not product["has_buy_box"]:
            product["winner_seller"]  = info.get("winner_seller", "")
            product["winner_url"]     = info.get("winner_url", "")
            product["winner_price"]   = info.get("winner_price", "")
            product["recommendation"] = compute_recommendation(
                product.get("winner_price", ""),
                product.get("lowest_msrp")
            )
        else:
            product["recommendation"] = ""
        all_products.append(product)

    dashboard_data = {
        "last_updated":  datetime.now(timezone.utc).isoformat(),
        "total_checked": len(inventory),
        "buy_box_owned": len(inventory) - len(flagged),
        "total_flagged": len(flagged),
        "products":      all_products,
    }

    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard", "data")
    os.makedirs(data_dir, exist_ok=True)
    with open(os.path.join(data_dir, "status.json"), "w") as f:
        json.dump(dashboard_data, f, indent=2)
    print(f"  Saved to dashboard/data/status.json")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
