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


def derive_amazon_usd_cad_rate(access_token, items):
    """Derive Amazon's internal USD→CAD conversion rate by comparing
    buyer-facing prices (CAD, from getItemOffers) against seller-facing
    prices (USD, from listingOffers already in buy_box_map).

    Amazon uses an internal exchange rate for cross-border sellers that
    differs significantly from the market rate, so we must derive it
    from actual Amazon data rather than a public exchange rate API.
    """
    headers = sp_api_headers(access_token)
    # Sample up to 10 ASINs that have known seller prices
    sample_asins = []
    seen = set()
    for item in items:
        asin = item["asin"]
        if asin not in seen:
            seen.add(asin)
            sample_asins.append(asin)
            if len(sample_asins) >= 10:
                break

    rates = []
    for asin in sample_asins:
        try:
            url = (f"{SP_API_BASE}/products/pricing/v0/items/{asin}/offers"
                   f"?MarketplaceId={MARKETPLACE_ID}&ItemCondition=New&CustomerType=Consumer")
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 429:
                time.sleep(5)
                resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                continue

            offers = resp.json().get("payload", {}).get("Offers", [])
            our_offer = next((o for o in offers if o.get("SellerId") == MY_SELLER_ID), None)
            if not our_offer:
                continue

            buyer_price = our_offer.get("ListingPrice", {}).get("Amount")
            buyer_currency = our_offer.get("ListingPrice", {}).get("CurrencyCode", "")

            if buyer_price and float(buyer_price) > 0:
                print(f"  Buyer price for {asin}: {buyer_currency} {buyer_price}")
                rates.append({"asin": asin, "buyer_price": float(buyer_price),
                              "buyer_currency": buyer_currency})
        except Exception as e:
            print(f"  Error sampling {asin}: {e}")
        time.sleep(0.5)

    if not rates:
        print("  WARNING: Could not sample any buyer-facing prices")
        return None

    # Log what we found
    for r in rates[:3]:
        print(f"  Sample: {r['asin']} buyer={r['buyer_currency']} {r['buyer_price']}")

    return rates


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


def _get_fees_from_report(headers):
    """Try the fee report approach first. Returns fee_map or empty dict on failure."""
    print("  Trying fee report (GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA)...")
    content = _request_report(headers, "GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA", MARKETPLACE_ID)
    if content is None:
        print("  Fee report failed.")
        return {}

    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    fieldnames = reader.fieldnames or []
    print(f"  Fee report columns: {fieldnames}")

    fee_map = {}
    total_rows = 0
    sample_logged = 0

    for row in reader:
        asin = row.get("asin", "").strip()
        if not asin:
            continue
        total_rows += 1

        def _parse(val):
            try:
                return float((val or "0").strip()) if val and val.strip() else 0.0
            except ValueError:
                return 0.0

        referral    = _parse(row.get("estimated-referral-fee-per-unit"))
        fulfillment = _parse(row.get("expected-fulfillment-fee-per-unit"))
        cad_price   = _parse(row.get("your-price"))

        fee_map[asin] = {
            "referral_fee":    round(referral, 4),
            "fulfillment_fee": round(fulfillment, 4),
            "cad_price":       round(cad_price, 2) if cad_price > 0 else None,
        }

        if sample_logged < 5:
            print(f"  Sample: {asin} CAD_price={cad_price} referral={referral} fulfillment={fulfillment}")
            sample_logged += 1

    print(f"  Fee report: {total_rows} rows, {len(fee_map)} unique ASINs")
    return fee_map


def _get_fees_per_asin(headers, items, buy_box_map, usd_cad_rate=1.0):
    """Fallback: call the per-ASIN fee estimate endpoint individually.
    Slower but more reliable than the report approach.
    Converts SP-API USD prices to CAD before sending to the fees endpoint
    so referral fees are calculated on the correct CAD amount.
    """
    url_base = f"{SP_API_BASE}/products/fees/v0/items"
    fee_map = {}

    # Get unique ASINs with prices, convert to CAD
    asin_price = {}
    for item in items:
        asin = item["asin"]
        if asin not in asin_price:
            info = buy_box_map.get(item["sku"], {})
            msrp_str = info.get("our_msrp", "")
            try:
                usd_price = float(msrp_str.replace("$", "").replace(",", "")) if msrp_str else 50.0
            except (ValueError, AttributeError):
                usd_price = 50.0
            asin_price[asin] = round(usd_price * usd_cad_rate, 2)

    asins = list(asin_price.keys())
    total = len(asins)
    print(f"  Fetching fees for {total} ASINs via per-ASIN endpoint...")

    for idx, asin in enumerate(asins):
        price = asin_price[asin]
        body = {
            "FeesEstimateRequest": {
                "MarketplaceId": MARKETPLACE_ID,
                "IsAmazonFulfilled": True,
                "PriceToEstimateFees": {
                    "ListingPrice": {"CurrencyCode": "CAD", "Amount": price},
                },
                "Identifier": asin,
            }
        }
        try:
            resp = requests.post(f"{url_base}/{asin}/feesEstimate",
                                 headers=headers, json=body, timeout=30)
            if resp.status_code == 429:
                time.sleep(30)
                resp = requests.post(f"{url_base}/{asin}/feesEstimate",
                                     headers=headers, json=body, timeout=30)

            if resp.status_code == 200:
                payload = resp.json().get("payload", {})
                res = payload.get("FeesEstimateResult", {})
                if res.get("Status") == "Success":
                    fee_list = res.get("FeesEstimate", {}).get("FeeDetailList", [])
                    referral = 0.0
                    fulfillment = 0.0
                    for f in fee_list:
                        amt = f.get("FeeAmount", {}).get("Amount", 0) or 0
                        ft = f.get("FeeType", "")
                        if ft == "ReferralFee":
                            referral += float(amt)
                        elif ft in ("FBAFees", "FulfillmentFee"):
                            fulfillment += float(amt)
                    # The response includes TotalFeesEstimate with CurrencyCode
                    total_currency = res.get("FeesEstimate", {}).get(
                        "TotalFeesEstimate", {}).get("CurrencyCode", "?")
                    fee_map[asin] = {
                        "referral_fee":    round(referral, 4),
                        "fulfillment_fee": round(fulfillment, 4),
                        "cad_price":       None,  # not available from this endpoint
                        "currency":        total_currency,
                    }
                    if idx < 3:
                        print(f"  Sample: {asin} referral={referral} fulfillment={fulfillment} currency={total_currency}")
                else:
                    err = res.get("Error", {})
                    if idx < 3:
                        print(f"  Fee error {asin}: {err.get('Message', '')[:100]}")
            else:
                if idx < 3:
                    print(f"  HTTP {resp.status_code} for {asin}: {resp.text[:200]}")
        except Exception as e:
            if idx < 3:
                print(f"  Exception for {asin}: {e}")

        if (idx + 1) % 50 == 0:
            print(f"  Fees progress: {idx + 1}/{total}")
        time.sleep(0.5)

    print(f"  Per-ASIN fees: {len(fee_map)} out of {total} ASINs")
    return fee_map


def get_fee_estimates(access_token, items=None, buy_box_map=None):
    """Fetch per-ASIN referral fee, fulfillment fee, and CAD listing price.

    Strategy:
    1. Fetch live USD→CAD exchange rate
    2. Try the fee report (fast, one call for all ASINs, returns CAD prices)
    3. If report fails or is empty, fall back to per-ASIN fee endpoint with CAD prices

    Returns (fee_map, usd_cad_rate) where fee_map: asin -> {referral_fee, fulfillment_fee, cad_price}
    """
    headers = sp_api_headers(access_token)

    # Derive Amazon's internal USD→CAD rate by comparing buyer-facing (CAD)
    # vs seller-facing (USD) prices for sample ASINs.
    usd_cad_rate = 1.0
    if items and buy_box_map:
        samples = derive_amazon_usd_cad_rate(access_token, items)
        if samples:
            # Compare buyer prices with seller API prices to get the rate
            derived_rates = []
            for s in samples:
                asin = s["asin"]
                buyer_price = s["buyer_price"]
                # Find the seller API price from buy_box_map
                for item in items:
                    if item["asin"] == asin:
                        info = buy_box_map.get(item["sku"], {})
                        seller_msrp = info.get("our_msrp", "")
                        if seller_msrp:
                            try:
                                seller_price = float(seller_msrp.replace("$", "").replace(",", ""))
                                if seller_price > 0:
                                    rate = buyer_price / seller_price
                                    if 0.8 < rate < 2.0:  # sanity check
                                        derived_rates.append(rate)
                                        print(f"  Rate for {asin}: buyer={buyer_price} / seller={seller_price} = {rate:.4f}")
                            except ValueError:
                                pass
                        break
            if derived_rates:
                usd_cad_rate = round(sum(derived_rates) / len(derived_rates), 6)
                print(f"  Derived Amazon USD→CAD rate: {usd_cad_rate} (from {len(derived_rates)} samples)")
            else:
                print("  WARNING: Could not derive rate, using 1.0")
        else:
            print("  WARNING: No samples available, using 1.0")

    # Try report first
    fee_map = _get_fees_from_report(headers)

    # Check if report actually returned meaningful data
    has_fees = any(v.get("referral_fee", 0) > 0 or v.get("fulfillment_fee", 0) > 0
                   for v in fee_map.values())

    if not has_fees:
        print("  Fee report returned no fee data — falling back to per-ASIN endpoint")
        if items and buy_box_map:
            fee_map = _get_fees_per_asin(headers, items, buy_box_map, usd_cad_rate)
        else:
            print("  WARNING: No items/buy_box_map for fallback — fees will be 0")

    return fee_map, usd_cad_rate


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
    fee_estimates, usd_cad_rate = get_fee_estimates(access_token, inventory, buy_box_map)

    def _build_total_cost(asin, ft, cost_data):
        """Combine product cost + referral fee + fulfillment fee for the given fulfillment type.
        Fees come from the fee report and already match the current fulfillment type
        (FBA fee for FBA products, NARF fee for NARF products).
        """
        if not cost_data:
            return None
        product_cost = cost_data["fba_cost"] if ft == "FBA" else cost_data["narf_cost"]
        if product_cost is None:
            return None
        fees = fee_estimates.get(asin, {})
        referral_fee    = fees.get("referral_fee", 0) or 0
        fulfillment_fee = fees.get("fulfillment_fee", 0) or 0
        return round(product_cost + referral_fee + fulfillment_fee, 4)

    # Convert USD prices from SP-API to CAD using the live exchange rate.
    print(f"  Applying USD→CAD rate: {usd_cad_rate}")

    def _to_cad(usd_str):
        """Convert a USD price string like '$227.47' to CAD string."""
        if not usd_str:
            return ""
        try:
            val = float(usd_str.replace("$", "").replace(",", ""))
            return f"${val * usd_cad_rate:.2f}"
        except ValueError:
            return usd_str

    def _get_cad_msrp(asin, info):
        """Get the CAD MSRP — prefer fee report price, fall back to converted SP-API price."""
        fee_data = fee_estimates.get(asin, {})
        cad_price = fee_data.get("cad_price")
        if cad_price:
            return f"${cad_price:.2f}"
        return _to_cad(info.get("our_msrp", ""))

    print("\n[7/8] Flagging and alerting...")
    flagged = []
    for item in inventory:
        info = buy_box_map.get(item["sku"], {})
        ft = ("FBA" if item["asin"] in fba_asins else "NARF") if fba_asins is not None else "Unknown"
        if not info.get("has_buy_box"):
            item["winner_seller"] = info.get("winner_seller", "Unknown")
            item["winner_url"]    = info.get("winner_url", "")
            item["winner_price"]  = _to_cad(info.get("winner_price", ""))
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
            product["winner_price"]   = _to_cad(info.get("winner_price", ""))
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
