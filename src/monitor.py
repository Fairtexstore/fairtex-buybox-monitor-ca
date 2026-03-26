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
SP_API_BASE    = "https://sellingpartnerapi-na.amazon.com"
SLACK_CHANNEL  = "C0AMDJ91151"
MY_SELLER_ID   = "A1LC1HJLF7IAWT"


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
    """Load product cost data from product_costs.csv. Returns dict keyed by ASIN."""
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "product_costs.csv")
    costs = {}
    try:
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                asin = row.get("ASIN", "").strip()
                if not asin:
                    continue
                total_cost_str = row.get("Total_Cost", "").strip().replace("$", "").replace(",", "")
                lowest_msrp_str = row.get("Lowest_MSRP_20pct_Profit", "").strip().replace("$", "").replace(",", "")
                try:
                    total_cost = float(total_cost_str) if total_cost_str else None
                except ValueError:
                    total_cost = None
                try:
                    lowest_msrp = float(lowest_msrp_str) if lowest_msrp_str else None
                except ValueError:
                    lowest_msrp = None
                costs[asin] = {"total_cost": total_cost, "lowest_msrp": lowest_msrp}
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
    """Request a report and wait for it to complete. Returns report content or None."""
    print(f"  Creating report: {report_type} for marketplace {marketplace_id}")
    resp = requests.post(
        f"{SP_API_BASE}/reports/2021-06-30/reports",
        headers=headers,
        json={"reportType": report_type, "marketplaceIds": [marketplace_id]},
        timeout=30,
    )
    print(f"  Create report response: {resp.status_code}")
    if resp.status_code == 429:
        print("  Rate limited, waiting 30s...")
        time.sleep(30)
        resp = requests.post(
            f"{SP_API_BASE}/reports/2021-06-30/reports",
            headers=headers,
            json={"reportType": report_type, "marketplaceIds": [marketplace_id]},
            timeout=30,
        )
        print(f"  Retry response: {resp.status_code}")
    if resp.status_code not in (200, 202):
        print(f"  Failed to create {report_type}: {resp.status_code}: {resp.text[:500]}")
        return None

    response_json = resp.json()
    print(f"  Create report response body: {json.dumps(response_json)[:300]}")
    report_id = response_json.get("reportId")
    if not report_id:
        print(f"  No reportId returned for {report_type}")
        return None
    print(f"  Report ID: {report_id}")

    # Poll until done
    doc_id = None
    for attempt in range(30):
        time.sleep(10)
        resp = requests.get(
            f"{SP_API_BASE}/reports/2021-06-30/reports/{report_id}",
            headers=headers,
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"  Poll attempt {attempt + 1}: HTTP {resp.status_code}")
            continue
        poll_data = resp.json()
        status = poll_data.get("processingStatus", "")
        print(f"  Report status: {status} (attempt {attempt + 1})")
        if status == "DONE":
            doc_id = poll_data.get("reportDocumentId")
            break
        if status in ("CANCELLED", "FATAL"):
            print(f"  Report failed: {status}")
            print(f"  Full response: {json.dumps(poll_data)[:500]}")
            return None
    if not doc_id:
        print("  Report timed out after 5 minutes")
        return None
    print(f"  Document ID: {doc_id}")

    # Get document URL
    resp = requests.get(
        f"{SP_API_BASE}/reports/2021-06-30/documents/{doc_id}",
        headers=headers,
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"  Failed to get report document: {resp.status_code}: {resp.text[:300]}")
        return None

    doc_info = resp.json()
    doc_url = doc_info.get("url")
    compression = doc_info.get("compressionAlgorithm")
    print(f"  Document compression: {compression or 'None'}")
    if not doc_url:
        print("  No document URL returned")
        return None

    # Download
    print("  Downloading report document...")
    resp = requests.get(doc_url, timeout=60)
    if resp.status_code != 200:
        print(f"  Failed to download report: {resp.status_code}")
        return None

    raw_bytes = resp.content
    print(f"  Downloaded {len(raw_bytes)} bytes")

    # Decompress if GZIP
    if compression == "GZIP":
        print("  Decompressing GZIP...")
        try:
            raw_bytes = gzip.decompress(raw_bytes)
            print(f"  Decompressed to {len(raw_bytes)} bytes")
        except Exception as e:
            print(f"  GZIP decompression failed: {e}")
            return None

    return raw_bytes.decode("utf-8", errors="replace")


def get_fulfillment_types(access_token):
    """Classify products as FBA or NARF using inventory-by-country data.

    Tries multiple report types since GET_FBA_INVENTORY_AGED_DATA returns
    FATAL for Canada marketplace.

    Approach 1: GET_AFN_INVENTORY_DATA_BY_COUNTRY
    - Shows inventory quantity per country
    - SKU with inventory in country=CA → FBA (physically in Canadian FCs)
    - SKU with inventory only in country=US → NARF (remote from US FCs)

    Approach 2: GET_FBA_INVENTORY_PLANNING_DATA (fallback)
    - Has age bucket columns like the aged data report
    - All age buckets = 0 → NARF, any > 0 → FBA

    Returns a dict: {sku: "FBA" or "NARF"}, or None if all reports failed.
    """
    headers = sp_api_headers(access_token)

    # --- Approach 1: Inventory by Country ---
    print("  Trying GET_AFN_INVENTORY_DATA_BY_COUNTRY...")
    content = _request_report(headers, "GET_AFN_INVENTORY_DATA_BY_COUNTRY", MARKETPLACE_ID)
    if content is not None:
        print(f"  Raw report preview (first 500 chars):")
        print(f"  {content[:500]}")
        reader = csv.DictReader(io.StringIO(content), delimiter="\t")
        fieldnames = reader.fieldnames or []
        print(f"  Report columns: {fieldnames}")

        sku_col = "seller-sku" if "seller-sku" in fieldnames else "sku"
        country_col = "country"
        # Amazon uses "quantity-for-local-fulfillment" in this report
        qty_col = next(
            (c for c in fieldnames if "quantity" in c.lower()),
            "quantity-for-local-fulfillment"
        )

        # Track which countries each SKU has inventory in
        sku_countries = {}  # {sku: set of countries with qty > 0}
        sample_logged = 0
        for row in reader:
            sku = row.get(sku_col, "").strip()
            country = row.get(country_col, "").strip().upper()
            qty_str = row.get(qty_col, "0").strip()
            if not sku:
                continue
            if sample_logged < 5:
                print(f"  Sample: SKU={sku}, country={country}, qty={qty_str}")
                sample_logged += 1
            try:
                qty = int(qty_str)
            except ValueError:
                qty = 0
            if qty > 0:
                if sku not in sku_countries:
                    sku_countries[sku] = set()
                sku_countries[sku].add(country)

        if sku_countries:
            fulfillment_types = {}
            fba_count = 0
            narf_count = 0
            for sku, countries in sku_countries.items():
                if "CA" in countries:
                    fulfillment_types[sku] = "FBA"
                    fba_count += 1
                else:
                    fulfillment_types[sku] = "NARF"
                    narf_count += 1
            print(f"  Classified {len(fulfillment_types)} SKUs: {fba_count} FBA, {narf_count} NARF")
            return fulfillment_types
        else:
            print("  Report returned no data rows")

    # --- Approach 2: Inventory Planning Data (has age columns) ---
    print("  Trying GET_FBA_INVENTORY_PLANNING_DATA...")
    content = _request_report(headers, "GET_FBA_INVENTORY_PLANNING_DATA", MARKETPLACE_ID)
    if content is not None:
        print(f"  Raw report preview (first 500 chars):")
        print(f"  {content[:500]}")
        reader = csv.DictReader(io.StringIO(content), delimiter="\t")
        fieldnames = reader.fieldnames or []
        print(f"  Report columns: {fieldnames}")

        sku_col = "sku" if "sku" in fieldnames else "seller-sku"
        age_columns = [
            "inv-age-0-to-90-days", "inv-age-91-to-180-days",
            "inv-age-181-to-270-days", "inv-age-271-to-365-days",
            "inv-age-365-plus-days",
        ]

        fulfillment_types = {}
        fba_count = 0
        narf_count = 0
        for row in reader:
            sku = row.get(sku_col, "").strip()
            if not sku:
                continue
            has_aged = any(
                int(row.get(col, "0").strip() or "0") > 0
                for col in age_columns
            )
            if has_aged:
                fulfillment_types[sku] = "FBA"
                fba_count += 1
            else:
                fulfillment_types[sku] = "NARF"
                narf_count += 1

        if fulfillment_types:
            print(f"  Classified {len(fulfillment_types)} SKUs: {fba_count} FBA, {narf_count} NARF")
            return fulfillment_types
        print("  Report returned no data rows")

    # --- Approach 3: Aged Data (original, fails for CA but try anyway) ---
    print("  Trying GET_FBA_INVENTORY_AGED_DATA as last resort...")
    content = _request_report(headers, "GET_FBA_INVENTORY_AGED_DATA", MARKETPLACE_ID)
    if content is not None:
        reader = csv.DictReader(io.StringIO(content), delimiter="\t")
        fieldnames = reader.fieldnames or []
        sku_col = "sku" if "sku" in fieldnames else "seller-sku"
        age_columns = [
            "inv-age-0-to-90-days", "inv-age-91-to-180-days",
            "inv-age-181-to-270-days", "inv-age-271-to-365-days",
            "inv-age-365-plus-days",
        ]
        fulfillment_types = {}
        fba_count = 0
        narf_count = 0
        for row in reader:
            sku = row.get(sku_col, "").strip()
            if not sku:
                continue
            has_aged = any(
                int(row.get(col, "0").strip() or "0") > 0
                for col in age_columns
            )
            fulfillment_types[sku] = "FBA" if has_aged else "NARF"
            if has_aged:
                fba_count += 1
            else:
                narf_count += 1
        if fulfillment_types:
            print(f"  Classified {len(fulfillment_types)} SKUs: {fba_count} FBA, {narf_count} NARF")
            return fulfillment_types

    print("  WARNING: All report approaches failed — cannot classify FBA vs NARF")
    return None


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
        time.sleep(2)

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

            # Extract our listing price
            price_source = our_offer
            winner = next((o for o in offers if o.get("IsBuyBoxWinner")), None)
            if not price_source and winner and winner.get("SellerId") == MY_SELLER_ID:
                price_source = winner
            our_msrp = ""
            if price_source:
                lp = price_source.get("ListingPrice", {}).get("Amount")
                if lp is not None:
                    our_msrp = f"${float(lp):.2f}"
                    asin_msrp[item["asin"]] = our_msrp

            we_have_it = (
                (our_offer and our_offer.get("IsBuyBoxWinner") is True) or
                (winner and winner.get("SellerId") == MY_SELLER_ID)
            )

            if we_have_it:
                result[sku] = {"has_buy_box": True, "our_msrp": our_msrp}
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
                    "winner_seller":  winner_seller,
                    "winner_url":     winner_url,
                    "winner_price":   winner_price,
                }

        if (i + batch_size) % 100 < batch_size:
            print(f"  Progress: {min(i + batch_size, total)}/{total}")

        time.sleep(3)

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
            time.sleep(0.5)
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


def main():
    print("=== Amazon CA Buy Box Monitor ===")
    print(f"Time (UTC): {datetime.now(timezone.utc).isoformat()}")

    print("\n[1/5] Fetching LWA access token...")
    access_token = get_lwa_access_token()
    print("  OK")

    print("\n[2/5] Fetching FBA inventory...")
    inventory = get_fba_inventory(access_token)
    print(f"  {len(inventory)} SKUs in stock.")

    if not inventory:
        send_slack_alert([], 0)
        return

    print("\n[3/7] Checking buy box and prices per SKU...")
    buy_box_map = check_buy_box(access_token, inventory)

    print("\n[4/7] Classifying FBA vs NARF...")
    fulfillment_types = get_fulfillment_types(access_token)
    # fulfillment_types is a dict {sku: "FBA"/"NARF"}, or None if report failed

    print("\n[5/7] Loading product cost data...")
    product_costs = load_product_costs()

    print("\n[6/7] Flagging and alerting...")
    flagged = []
    for item in inventory:
        info = buy_box_map.get(item["sku"], {})
        if not info.get("has_buy_box"):
            item["winner_seller"] = info.get("winner_seller", "Unknown")
            item["winner_url"]    = info.get("winner_url", "")
            item["winner_price"]  = info.get("winner_price", "")
            cost_data = product_costs.get(item["asin"])
            if cost_data:
                item["total_cost"]  = cost_data["total_cost"]
                item["lowest_msrp"] = cost_data["lowest_msrp"]
                item["recommendation"] = compute_recommendation(item["winner_price"], cost_data["lowest_msrp"])
            else:
                item["total_cost"]     = None
                item["lowest_msrp"]    = None
                item["recommendation"] = ""
            flagged.append(item)
    print(f"  Flagged: {len(flagged)}")
    for p in flagged:
        rec_str = f" | Rec: {p['recommendation']}" if p.get("recommendation") else ""
        print(f"    - {p['sku']} | {p['asin']} | stock: {p['stock']} | winner: {p['winner_seller']} @ {p['winner_price']}{rec_str}")

    send_slack_alert(flagged, len(inventory), "https://fairtex-buybox-monitor-ca.vercel.app/")

    # Save results to JSON for the Vercel dashboard
    print("\n[7/7] Saving dashboard data...")
    all_products = []
    for item in inventory:
        info = buy_box_map.get(item["sku"], {})
        cost_data = product_costs.get(item["asin"])
        product = {
            "sku":              item["sku"],
            "asin":             item["asin"],
            "name":             item["name"],
            "stock":            item["stock"],
            "our_msrp":         info.get("our_msrp", ""),
            "has_buy_box":      info.get("has_buy_box", True),
            "total_cost":       cost_data["total_cost"] if cost_data else None,
            "lowest_msrp":      cost_data["lowest_msrp"] if cost_data else None,
            "fulfillment_type": fulfillment_types.get(item["sku"], "NARF") if fulfillment_types is not None else "Unknown",
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
